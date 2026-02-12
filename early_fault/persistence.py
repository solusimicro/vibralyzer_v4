class PersistenceChecker:
    def __init__(
        self,
        watch_persistence: int,
        warning_persistence: int,
        alarm_persistence: int,
        hysteresis_clear: int,
    ):
        self.watch_limit = watch_persistence
        self.warning_limit = warning_persistence
        self.alarm_limit = alarm_persistence
        self.hysteresis_clear = hysteresis_clear

        self.counter = 0
        self.current_state = "NORMAL"

    def update(self, evidence: str) -> str:
        """
        evidence: NORMAL / WATCH / WARNING / ALARM
        """

        if evidence == "NORMAL":
            self.counter -= 1
            if self.counter <= -self.hysteresis_clear:
                self.current_state = "NORMAL"
                self.counter = 0
            return self.current_state

        # escalation
        if evidence == "WATCH":
            self.counter += 1
            if self.counter >= self.watch_limit:
                self.current_state = "WATCH"

        elif evidence == "WARNING":
            self.counter += 1
            if self.counter >= self.warning_limit:
                self.current_state = "WARNING"

        elif evidence == "ALARM":
            self.counter += 1
            if self.counter >= self.alarm_limit:
                self.current_state = "ALARM"

        return self.current_state


