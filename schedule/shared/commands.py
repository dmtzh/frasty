from dataclasses import dataclass

from .domainschedule import CronSchedule

@dataclass(frozen=True)
class ClearCommand:
    '''Clear task schedule command'''

@dataclass(frozen=True)
class SetCommand:
    '''Set task schedule command'''
    schedule: CronSchedule

type Command = ClearCommand | SetCommand