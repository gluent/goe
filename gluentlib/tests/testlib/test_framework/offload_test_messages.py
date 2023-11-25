"""Wrapper over OffloadMessages that has useful methods for inspecting the messages or log."""


class OffloadTestMessages:
    def __init__(self, messages):
        self._messages = messages

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def get_lines_from_log(
        self, search_text, search_from_text="", max_matches=None
    ) -> list:
        """Searches for text in the logfile starting from search_from_text
        or the top of the file if search_from_text is blank.
        Returns all matching lines (up to max_matches).
        Be careful adding logging to this method, we can't log search_text otherwise
        we put the very thing we are searching for in the log.
        """
        log_file = self.get_log_fh_name()
        if not log_file:
            return []
        start_found = False if search_from_text else True
        matches = []
        lf = open(log_file, "r")
        for line in lf:
            if not start_found:
                start_found = search_from_text in line
            else:
                if search_text in line:
                    matches.append(line)
                    if max_matches and len(matches) >= max_matches:
                        return matches
        return matches

    def get_line_from_log(self, search_text, search_from_text="") -> str:
        matches = self.get_lines_from_log(
            search_text, search_from_text=search_from_text, max_matches=1
        )
        return matches[0] if matches else None

    def text_in_messages(self, log_text) -> bool:
        return bool([_ for _ in self._messages.get_messages() if log_text in _])

    ###########################################################################
    # PASSTHROUGH PUBLIC METHODS
    ###########################################################################

    def debug(self, *args, **kwargs):
        return self._messages.debug(*args, **kwargs)

    def get_log_fh(self):
        return self._messages.get_log_fh()

    def get_log_fh_name(self):
        return self._messages.get_log_fh_name()

    def get_events(self):
        return self._messages.get_events()

    def get_messages(self):
        return self._messages.get_messages()

    def info(self, *args, **kwargs):
        return self._messages.info(*args, **kwargs)

    def log(self, *args, **kwargs):
        return self._messages.log(*args, **kwargs)

    def warning(self, *args, **kwargs):
        return self._messages.warning(*args, **kwargs)

    @property
    def execution_id(self):
        return self._messages.execution_id

    @execution_id.setter
    def execution_id(self, new_value):
        self._messages.execution_id = new_value
