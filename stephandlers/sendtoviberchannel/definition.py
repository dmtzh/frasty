from dataclasses import dataclass

from expression import Result

from shared.customtypes import IdValue
from shared.domaindefinition import StepDefinition
from shared.utils.parse import parse_value, parse_non_empty_str
from shared.utils.result import ResultTag
from shared.validation import ValueInvalid, ValueError as ValueErr
from stepdefinitions.shared import ListOfDictData

class ViberChannelIdValue(IdValue):
    '''Viber channel id'''

@dataclass(frozen=True)
class SendToViberChannelConfig:
    channel_id: ViberChannelIdValue
    title: str

    @staticmethod
    def create(channel_id: str, title: str) -> Result["SendToViberChannelConfig", list[ValueErr]]:
        def validate_channel_id() -> Result[ViberChannelIdValue, list[ValueErr]]:
            return parse_value(channel_id, "channel_id", ViberChannelIdValue.from_value_with_checksum)\
                .map_error(lambda _: [ValueInvalid("channel_id")])
        def validate_title() -> Result[str, list[ValueErr]]:
            return parse_value(title, "title", parse_non_empty_str).map_error(lambda _: [ValueInvalid("title")])
        channel_id_res = validate_channel_id()
        title_res = validate_title()
        match channel_id_res, title_res:
            case Result(tag=ResultTag.OK, ok=valid_channel_id), Result(tag=ResultTag.OK, ok=valid_title):
                config = SendToViberChannelConfig(valid_channel_id, valid_title)
                return Result.Ok(config)
            case _:
                errors = channel_id_res.swap().default_value([]) + title_res.swap().default_value([])
                return Result.Error(errors)
    
    def to_dict(self):
        return {"channel_id": self.channel_id.to_value_with_checksum(), "title": self.title}

class SendToViberChannel(StepDefinition[SendToViberChannelConfig]):
    def __init__(self, config: SendToViberChannelConfig):
        super().__init__(config=config)
    
    @property
    def input_type(self) -> type:
        return ListOfDictData
    
    @property
    def output_type(self) -> type:
        return ListOfDictData
    
    @staticmethod
    def create(channel_id: str, title: str):
        config_res = SendToViberChannelConfig.create(channel_id, title)
        return config_res.map(SendToViberChannel)
    
    @staticmethod
    def validate_input(data):
        validated_input = ListOfDictData.from_list(data)
        return validated_input.map(ListOfDictData.to_list)