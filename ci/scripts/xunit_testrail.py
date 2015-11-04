# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This plugin is an extended version of the xunit plugin which also pushed to testrail each test updating it on the fly

It was designed for the `Hudson`_ continuous build system but will
probably work for anything else that understands an XUnit-formatted XML
representation of test results.

Add this shell command to your builder ::

    nosetests --with-xunit

And by default a file named nosetests.xml will be written to the
working directory.

In a Hudson builder, tick the box named "Publish JUnit test result report"
under the Post-build Actions and enter this value for Test report XMLs::

    **/nosetests.xml

If you need to change the name or location of the file, you can set the
``--xunit-file`` option.

Here is an abbreviated version of what an XML test report might look like::

    <?xml version="1.0" encoding="UTF-8"?>
    <testsuite name="nosetests" tests="1" errors="1" failures="0" skip="0">
        <testcase classname="path_to_test_suite.TestSomething"
                  name="test_it" time="0">
            <error type="exceptions.TypeError" message="oops, wrong type">
            Traceback (most recent call last):
            ...
            TypeError: oops, wrong type
            </error>
        </testcase>
    </testsuite>

.. _Hudson: https://hudson.dev.java.net/

"""

import os
import re
import sys
import inspect
import datetime
import traceback
import ConfigParser
from time import time
from xml.sax import saxutils
from StringIO import StringIO

from ci.scripts import testrailapi
from nose.exc import SkipTest
from nose.loader import TestLoader
from nose.plugins.base import Plugin
from nose.pyversion import force_unicode, format_exception

import logging
log = logging.getLogger('xunit.testrail')


# Invalid XML characters, control characters 0-31 sans \t, \n and \r
CONTROL_CHARACTERS = re.compile(r"[\000-\010\013\014\016-\037]")

CRASH_FILE_LOG = "/var/log/testrunner_crash"

AUTOTEST_DIR = os.path.join(os.sep, "opt", "OpenvStorage", "ci")
TESTS_DIR = os.path.join(AUTOTEST_DIR, "tests")
CONFIG_DIR = os.path.join(AUTOTEST_DIR, "config")


def xml_safe(value):
    """Replaces invalid XML characters with '?'."""
    return CONTROL_CHARACTERS.sub('?', value)


def escape_cdata(cdata):
    """Escape a string for an XML CDATA section."""
    return xml_safe(cdata).replace(']]>', ']]>]]&gt;<![CDATA[')


def nice_classname(obj):
    """Returns a nice name for class object or class instance.

        >>> nice_classname(Exception()) # doctest: +ELLIPSIS
        '...Exception'
        >>> nice_classname(Exception)
        'exceptions.Exception'

    """
    if inspect.isclass(obj):
        cls_name = obj.__name__
    else:
        cls_name = obj.__class__.__name__
    mod = inspect.getmodule(obj)
    if mod:
        name = mod.__name__
        # jython
        if name.startswith('org.python.core.'):
            name = name[len('org.python.core.'):]
        return "%s.%s" % (name, cls_name)
    else:
        return cls_name


def exc_message(exc_info):
    """Return the exception's message."""
    exc = exc_info[1]
    if exc is None:
        # str exception
        result = exc_info[0]
    else:
        try:
            result = str(exc)
        except UnicodeEncodeError:
            try:
                result = unicode(exc)
            except UnicodeError:
                # Fallback to args as neither str nor
                # unicode(Exception(u'\xe6')) work in Python < 2.6
                result = exc.args[0]
    return xml_safe(result)


def name_to_testrail_format(test_name):
    match = re.search("c\d+_(.+)", test_name)
    case_name = match.groups()[0] if match else test_name
    return case_name


def format_durations(dur):
    if not dur:
        return ""
    nice_time = lambda x: str(datetime.timedelta(seconds=int(x)))
    splits = dur.split("|")
    if len(splits) == 3:
        return "Past Runs Avg: " + nice_time(splits[2]) + " Min: " + nice_time(splits[0]) +\
               " Max: " + nice_time(splits[1])
    else:
        return ""


class Tee(object):
    def __init__(self, encoding, *args):
        self._encoding = encoding
        self._streams = args

    def write(self, data):
        data = force_unicode(data, self._encoding)
        for s in self._streams:
            s.write(data)

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def flush(self):
        for s in self._streams:
            s.flush()

    def isatty(self):
        return False


class xunit_testrail(Plugin):
    """This plugin provides test results in the standard XUnit XML format."""
    name = 'xunit_testrail'
    score = 2000
    encoding = 'UTF-8'
    error_report_file = None

    def __init__(self):
        self._timer = 0.0
        self._capture_stack = []
        self._currentStdout = None
        self._currentStderr = None
        self.case_item = ''
        self.config = ''
        self.durations = ''
        self.enableOpt = 'enable_plugin_xunit_testrail'
        self.errorlist = []
        self.existingPlan = ''
        self.fullsuite_name = ''
        self.hypervisor = ''
        self.plan = ''
        self.project = ''
        self.project_id = ''
        self.projectIni = ConfigParser.ConfigParser()
        self.projectName = ''
        self.run_id = None
        self.stats = {'errors': 0,
                      'failures': 0,
                      'passes': 0,
                      'skipped': 0}
        self.suite_name = ''
        self.test_id = ''
        self.testrailIp = ''
        self.testrailApi = ''
        self.testsCaseIdsToSelect = ''
        self.version = ''

        self.blockedStatus = ''
        self.failedStatus = ''
        self.passedStatus = ''
        self.skippedStatus = ''
        self.ongoingStatus = ''

    def _time_taken(self):
        if hasattr(self, '_timer'):
            taken = time() - self._timer
        else:
            # test died before it ran (probably error in setup())
            # or success/failure added before test started probably
            # due to custom TestResult munging
            taken = 0.0
        return taken

    def _quoteattr(self, attr):
        """Escape an XML attribute. Value can be unicode."""
        attr = xml_safe(attr)
        if isinstance(attr, unicode):
            attr = attr.encode(self.encoding)
        return saxutils.quoteattr(attr)

    def options(self, parser, env):
        """Sets additional command line options."""
        Plugin.options(self, parser, env)
        parser.add_option('--xunit_file2',
                          action='store',
                          dest='xunit_file2',
                          metavar="FILE",
                          default=env.get('NOSE_XUNIT_FILE', 'nosetests.xml'),
                          help=("Path to xml file to store the xunit report in. "
                                "Default is nosetests.xml in the working directory "
                                "[NOSE_XUNIT_FILE]"))

        parser.add_option('--testrail-ip',
                          action="store",
                          dest="testrailIp",
                          metavar="FILE",
                          default="",
                          help="Url of testrail server")

        parser.add_option('--testrail-key',
                          action="store",
                          dest="testrailKey",
                          metavar="FILE",
                          default="",
                          help="Key authentication")

        parser.add_option('--project-name',
                          action="store",
                          dest="projectName",
                          metavar="FILE",
                          default="Open vStorage Engineering",
                          help="Testrail project name")

        parser.add_option('--push-name',
                          action="store",
                          dest="pushName",
                          metavar="FILE",
                          default="AT push results",
                          help="Testrail push name")

        parser.add_option('--description',
                          action="store",
                          dest="description",
                          metavar="FILE",
                          default="",
                          help="Testrail description")

        parser.add_option('--plan-id',
                          action="store",
                          dest="planId",
                          metavar="FILE",
                          default="",
                          help="Existing plan id")

    def configure(self, options, config):
        """Configures the xunit plugin."""
        Plugin.configure(self, options, config)
        self.config = config
        self.error_report_file = open(options.xunit_file2, 'w')

        project_mapping = os.path.join(CONFIG_DIR, "project_testsuite_mapping.cfg")
        self.projectIni.read(project_mapping)
        self.testrailIp = options.testrailIp
        if self.testrailIp:
            self.testrailApi = testrailapi.TestrailApi(self.testrailIp, key=options.testrailKey)

            all_statuses = self.testrailApi.get_statuses()
            self.ongoingStatus = [s for s in all_statuses if s['name'].lower() == 'ongoing'][0]
            self.passedStatus = [s for s in all_statuses if s['name'].lower() == 'passed'][0]
            self.failedStatus = [s for s in all_statuses if s['name'].lower() == 'failed'][0]
            self.skippedStatus = [s for s in all_statuses if s['name'].lower() == 'skipped'][0]
            self.blockedStatus = [s for s in all_statuses if s['name'].lower() == 'blocked'][0]

            name_splits = options.pushName.split("_")
            name = name_splits[0]
            today = datetime.datetime.today()
            name += "_" + today.strftime('%a %b %d %H:%M:%S')

            self.version = name_splits[0]
            self.hypervisor = name_splits[2]
            self.projectName = options.projectName

            self.project = self.testrailApi.get_project_by_name(self.projectName)
            self.project_id = self.project['id']

            self.existingPlan = bool(options.planId)
            if options.planId:
                plan = self.testrailApi.get_plan(options.planId)
                os.write(1, "\nContinuing with plan {0}\n".format(plan['url']))
            else:
                milestone_id = None
                description = options.description

                plan = self.testrailApi.add_plan(self.project_id, name, description, milestone_id or None)
                os.write(1, "\nNew test plan: " + plan['url'] + "\n")
            self.plan = plan
            self.suite_name = ""
            self.testsCaseIdsToSelect = list()

    def report(self, stream):
        """Writes an Xunit-formatted XML file
        The file includes a report of test errors and failures.
        """
        self.stats['encoding'] = self.encoding
        self.stats['total'] = (self.stats['errors'] + self.stats['failures']
                               + self.stats['passes'] + self.stats['skipped'])
        self.error_report_file.write(
            '<?xml version="1.0" encoding="%(encoding)s"?>'
            '<testsuite name="nosetests" tests="%(total)d" '
            'errors="%(errors)d" failures="%(failures)d" '
            'skip="%(skipped)d">' % self.stats)
        self.error_report_file.write(''.join(self.errorlist))
        self.error_report_file.write('</testsuite>')
        self.error_report_file.close()
        if self.config.verbosity > 1:
            stream.writeln("-" * 70)
            stream.writeln("XML: %s" % self.error_report_file.name)

    def _start_capture(self):
        log.info('_start_capture...')
        self._capture_stack.append((sys.stdout, sys.stderr))
        self._currentStdout = StringIO()
        self._currentStderr = StringIO()
        sys.stdout = Tee(self.encoding, self._currentStdout, sys.stdout)
        sys.stderr = Tee(self.encoding, self._currentStderr, sys.stderr)

    def startContext(self, context):
        pass

    def stopContext(self, context):
        pass

    def beforeTest(self, test):
        log.info('beforeTest...')
        """Initializes a timer before starting a test."""
        self._timer = time()
        self._start_capture()

    def _end_capture(self):
        log.info('_end_capture...')
        if self._capture_stack:
            import pprint
            pprint.pprint(self._capture_stack)
            sys.stdout, sys.stderr = self._capture_stack.pop()

    def afterTest(self, test):
        log.info('afterTest...')
        self._end_capture()
        self._currentStdout = None
        self._currentStderr = None

    def finalize(self, test):
        while self._capture_stack:
            self._end_capture()

    def _get_captured_stdout(self):
        if self._currentStdout:
            value = self._currentStdout.getvalue()
            if value:
                return '<system-out><![CDATA[%s]]></system-out>' % escape_cdata(value)
        return ''

    def _get_captured_stderr(self):
        if self._currentStderr:
            value = self._currentStderr.getvalue()
            if value:
                return '<system-err><![CDATA[%s]]></system-err>' % escape_cdata(value)
        return ''

    def startTest(self, test):
        """Initializes a timer before starting a test."""
        test_id = test.id()
        log.log(2, str(test))
        if self.testrailIp:
            try:
                test_name = test_id.split('.')[-1]
                suite_dir = test_id.split('.')[-3]
                suite_name = self.projectIni.get(self.projectName, suite_dir).split(';')[0]
                section_names = self.projectIni.get(self.projectName, suite_dir).split(';')[1].split(',')
                section_name = section_names[-1]

                fullsuite_name = suite_name + ",".join(section_names)
                suite = self.testrailApi.get_suite_by_name(self.project_id, suite_name)
                suite_id = suite['id']
                section = self.testrailApi.get_section_by_name(self.project_id, suite_id, section_name)
                section_id = section['id']

                all_cases = self.testrailApi.get_cases(self.project_id, suite_id)
                all_case_names = [c['title'] for c in all_cases]

                is_new_run_to_be_created = False
                if fullsuite_name != self.fullsuite_name:
                    is_new_run_to_be_created = True

                    all_tests = [t for c in TestLoader().loadTestsFromDir(os.path.dirname(test.context.__file__)) for t in c._tests]
                    all_testnames = [name_to_testrail_format(t.id().split('.')[-1]) for t in all_tests]
                    os.write(1, str(all_testnames) + "\n")

                    self.fullsuite_name = fullsuite_name
                    for test_name in all_testnames:
                        for case in all_cases:
                            found = False
                            if case['section_id'] == section_id and case['title'] == test_name:
                                found = True
                                break
                        if not found:
                            self.testrailApi.add_case(section_id=section_id, title=test_name)
                        all_cases = self.testrailApi.get_cases(self.project_id, suite_id)

                case_name = name_to_testrail_format(test_name)
                case_item = [caseObj for caseObj in all_cases if
                             caseObj['section_id'] == section_id and caseObj['title'] == case_name]
                if not case_item:
                    raise Exception("Could not find case name %s on testrail" % case_name)
                self.case_item = case_item[0]

                run_id = self.run_id
                if is_new_run_to_be_created:
                    if self.existingPlan:
                        run = [r for e in self.plan['entries'] for r in e['runs'] if r['suite_id'] == suite_id]
                        if run:
                            run_id = run[0]['id']
                    if run_id is None:
                        self.testsCaseIdsToSelect = [c['id'] for c in all_cases if c['title'] in all_case_names]
                        entry = self.testrailApi.add_plan_entry(self.plan['id'], suite_id, suite_name,
                                                                include_all=False, case_ids=self.testsCaseIdsToSelect)
                        run_id = entry['runs'][0]['id']
                    self.run_id = run_id

                all_tests_for_run = self.testrailApi.get_tests(run_id)
                test = [t for t in all_tests_for_run if t['case_id'] == self.case_item['id']][0]
                self.test_id = test['id']
                self.durations = self.case_item.get("custom_at_avg_duration", "")

                test_status = self.ongoingStatus['id']
                self.testrailApi.add_result(test_id=self.test_id, status_id=test_status, comment='',
                                            version=self.version, custom_fields={'custom_hypervisor': self.hypervisor})

                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                os.write(1, now + "|" + format_durations(self.durations) + "-->")

            except:
                etype, value, tb = sys.exc_info()
                exception_text = str(traceback.format_exception(etype, value, tb))
                with open(CRASH_FILE_LOG, "a") as f:
                    f.write(exception_text + "\n" +
                            "\ntestname: " + test_name +
                            "\nsuite_name: " + suite_name +
                            "\nsection_names: {0}".format(', '.join(section_names)) +
                            "\nsection_name: " + section_name +
                            "\nall_cases: {0}".format(', '.join(case['title'] for case in all_cases)) +
                            "\ntest_id: " + test_id +
                            "\n\n")

    def addError(self, test, err, capt=None):
        """
        Add error output to Xunit report.
        """
        taken = self._time_taken()

        if issubclass(err[0], SkipTest):
            result_type = 'skipped'
            self.stats['skipped'] += 1
        else:
            result_type = 'error'
            self.stats['errors'] += 1
        tb = ''.join(traceback.format_exception(*err))
        test_id = test.id()
        self.errorlist.append(
            '<testcase classname=%(cls)s name=%(name)s time="%(taken)d">'
            '<%(type)s type=%(errtype)s message=%(message)s><![CDATA[%(tb)s]]>'
            '</%(type)s></testcase>' %
            {'cls': self._quoteattr('.'.join(test_id.split('.')[:-1])),
             'name': self._quoteattr(test_id.split('.')[-1]),
             'taken': taken,
             'type': result_type,
             'errtype': self._quoteattr(nice_classname(err[0])),
             'message': self._quoteattr(exc_message(err)),
             'tb': escape_cdata(tb),
            })

        if self.testrailIp:
            elapsed = '%ss' % (int(taken) or 1)

            try:
                if str(result_type) == 'skipped':
                    if "BLOCKED" in str(err):
                        test_status = self.blockedStatus['id']
                    else:
                        test_status = self.skippedStatus['id']
                else:
                    test_status = self.failedStatus['id']

                self.testrailApi.add_result(test_id=self.test_id, status_id=test_status, comment=exc_message(err),
                                            version=self.version, elapsed=elapsed,
                                            custom_fields={'custom_hypervisor': self.hypervisor})
            except:
                etype, value, tb = sys.exc_info()
                exception_text = str(traceback.format_exception(etype, value, tb))
                with open(CRASH_FILE_LOG, "a") as f:
                    f.write(exception_text + "\n\n")

    def addFailure(self, test, err):
        """Add failure output to Xunit report.
        """
        taken = self._time_taken()
        tb = ''.join(traceback.format_exception(*err))
        self.stats['failures'] += 1
        test_id = test.id()
        self.errorlist.append(
            '<testcase classname=%(cls)s name=%(name)s time="%(taken)d">'
            '<failure type=%(errtype)s message=%(message)s><![CDATA[%(tb)s]]>'
            '</failure></testcase>' %
            {'cls': self._quoteattr('.'.join(test_id.split('.')[:-1])),
             'name': self._quoteattr(test_id.split('.')[-1]),
             'taken': taken,
             'errtype': self._quoteattr(nice_classname(err[0])),
             'message': self._quoteattr(exc_message(err)),
             'tb': escape_cdata(tb),
            })

        if self.testrailIp:
            elapsed = (int(taken) or 1)

            try:
                if type == 'skipped':
                    test_status = self.skippedStatus['id']
                else:
                    test_status = self.failedStatus['id']

                self.testrailApi.add_result(test_id=self.test_id,
                                            status_id=test_status,
                                            comment=exc_message(err),
                                            version=self.version,
                                            elapsed='%ss' % elapsed,
                                            custom_fields={'custom_hypervisor': self.hypervisor})
            except:
                etype, value, tb = sys.exc_info()
                exception_text = str(traceback.format_exception(etype, value, tb))
                with open(CRASH_FILE_LOG, "a") as f:
                    f.write(exception_text + "\n\n")

    def addSuccess(self, test, capt=None):
        """Add success output to Xunit report.
        """
        taken = self._time_taken()
        self.stats['passes'] += 1
        test_id = test.id()
        self.errorlist.append(
            '<testcase classname=%(cls)s name=%(name)s '
            'time="%(taken)d" />' %
            {'cls': self._quoteattr('.'.join(test_id.split('.')[:-1])),
             'name': self._quoteattr(test_id.split('.')[-1]),
             'taken': taken,
             'systemout': self._get_captured_stdout(),
             'systemerr': self._get_captured_stderr(),
             })
        if self.testrailIp:
            elapsed = (int(taken) or 1)

            try:
                results = self.testrailApi.get_results(self.test_id)
                if any([r['status_id'] == self.failedStatus['id'] for r in results]):
                    return

                self.testrailApi.add_result(test_id=self.test_id,
                                            status_id=self.passedStatus['id'],
                                            comment="",
                                            version=self.version,
                                            elapsed='%ss' % elapsed,
                                            custom_fields={'custom_hypervisor': self.hypervisor})
                if self.durations:
                    time_statistics = self.case_item['custom_at_avg_duration']
                    time_statistics = map(int, time_statistics.split("|"))

                    time_statistics[2] = (time_statistics[2] + elapsed) / 2
                    if elapsed < time_statistics[0]:
                        time_statistics[0] = elapsed
                    if elapsed > time_statistics[1]:
                        time_statistics[1] = elapsed
                else:
                    time_statistics = [elapsed, elapsed, elapsed]

                self.testrailApi.update_case(case_id=self.case_item['id'],
                                             custom_fields={'custom_at_avg_duration': "|".join(map(str,
                                                                                                   time_statistics))})
            except:
                etype, value, tb = sys.exc_info()
                exception_text = str(traceback.format_exception(etype, value, tb))
                with open(CRASH_FILE_LOG, "a") as f:
                    f.write(exception_text + "\n\n")
