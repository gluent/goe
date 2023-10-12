from datetime import datetime
import os


class QueryHintBuilder:
    def __init__(self):
        self.hints = []

    def parallel(self, num):
        self.hints.append("PARALLEL({})".format(num))
        return self

    def monitor(self):
        self.hints.append("MONITOR")
        return self

    def query_monitor(self, monitor: bool):
        if monitor is not None:
            if monitor:
                self.hints.append("GLUENT_QUERY_MONITOR")
            else:
                self.hints.append("GLUENT_NO_QUERY_MONITOR")
        return self

    def unique_hint(self, unique_hint=None):
        if unique_hint:
            self.hints.append(unique_hint)
        else:
            self.hints.append(datetime.now().strftime('%Y%m%d%H%M%S%f'))
        return self

    def default_teamcity_hints(self):
        keys = list(os.environ.keys())
        if 'TEAMCITY_VERSION' not in keys:
            return
        self.gluent_trace('teamcity.buildConfName', os.environ['TEAMCITY_BUILDCONF_NAME'])
        self.gluent_trace('teamcity.projectName', os.environ['TEAMCITY_PROJECT_NAME'])
        self.gluent_trace('teamcity.build.number', os.environ['BUILD_NUMBER'])
        if 'BRANCH' in keys: self.gluent_trace("teamcity.build.branch", os.environ['BRANCH'])
        return self

    def __str__(self):
        return "/*+ " + " ".join(self.hints) + " */"

    def str(self):
        return str(self)
