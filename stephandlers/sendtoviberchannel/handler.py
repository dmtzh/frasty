import asyncio
from dataclasses import dataclass

import aiohttp
from expression import Result

from shared.completedresult import CompletedWith
from shared.customtypes import Error
from shared.infrastructure.storage.repository import NotFoundError, StorageError
from shared.utils.asyncresult import async_ex_to_error_result, async_result, coroutine_result

from config import ViberApiConfig
from .definition import ViberChannelIdValue
from .viberchannelsstore import viber_channels_storage, ViberChannel

_SEND_TO_VIBER_CHANNEL_KEY = "send_to_viber_channel"

class SendToViberChannelHandlerStorageError(StorageError):
    '''Unexpected send to viber channel handler storage error'''

@dataclass(frozen=True)
class SendToViberChannelCommand:
    channel_id: ViberChannelIdValue
    title: str
    messages: list[dict]

@dataclass(frozen=True)
class ViberTextMessage:
    text: str
    @staticmethod
    def from_dict(title: str, message: dict):
        msg = ", ".join(f"{key} {value}" for key, value in message.items() if key != _SEND_TO_VIBER_CHANNEL_KEY)
        msg_with_title = f"{title} {msg}"
        return ViberTextMessage(msg_with_title)

class ViberChannelUnexpectedError(Error):
    '''Unexpected error in viber channel'''

@async_result
@async_ex_to_error_result(SendToViberChannelHandlerStorageError.from_exception)
async def get_viber_channel(id: ViberChannelIdValue) -> Result[ViberChannel, NotFoundError]:
    opt_channel = await viber_channels_storage.get(id)
    match opt_channel:
        case None:
            return Result.Error(NotFoundError(f"Viber channel {id.to_value_with_checksum()} not found"))
        case channel:
            return Result.Ok(channel)

@async_result
@async_ex_to_error_result(ViberChannelUnexpectedError.from_exception)
async def send_to_viber_channel(viber_api_config: ViberApiConfig, channel: ViberChannel, cmd: SendToViberChannelCommand):
    @async_ex_to_error_result(ViberChannelUnexpectedError.from_exception)
    async def send_text_message(session: aiohttp.ClientSession, timeout: aiohttp.ClientTimeout, message: ViberTextMessage) -> Result[None, ViberChannelUnexpectedError]:
        request_json = {
            "text": message.text,
            "auth_token": channel.auth_token,
            "from": channel.from_,
            "type": "text"
        }
        async with session.request(method=viber_api_config.http_method, url=viber_api_config.url.value, json=request_json, timeout=timeout) as response:
            json = await response.json()
            match json["status"]:
                case 0:
                    return Result.Ok(None)
                case failed_status_num:
                    return Result.Error(ViberChannelUnexpectedError(f"Send failed with error {json["status_message"]} ({failed_status_num})"))
    def update_message_status(msg: dict, channel_id: ViberChannelIdValue, send_res: Result):
        status_msg = send_res.map(lambda _: "Success").default_with(str)
        msg.setdefault(_SEND_TO_VIBER_CHANNEL_KEY, {})[channel_id.to_value_with_checksum()] = status_msg
    tasks = []
    async with aiohttp.ClientSession() as session:
        timeout_15_seconds = aiohttp.ClientTimeout(total=15)
        for msg in cmd.messages:
            viber_text_msg = ViberTextMessage.from_dict(cmd.title, msg)
            task = send_text_message(session, timeout_15_seconds, viber_text_msg)
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        for msg, send_res in zip(cmd.messages, results):
            update_message_status(msg, cmd.channel_id, send_res)
        return cmd.messages

@coroutine_result[NotFoundError | SendToViberChannelHandlerStorageError | ViberChannelUnexpectedError]()
async def send_to_viber_channel_workflow(viber_api_config: ViberApiConfig, cmd: SendToViberChannelCommand):
    viber_channel = await get_viber_channel(cmd.channel_id)
    processed_messages = await send_to_viber_channel(viber_api_config, viber_channel, cmd)
    return processed_messages

async def handle(viber_api_config: ViberApiConfig, cmd: SendToViberChannelCommand) -> CompletedWith.Data | CompletedWith.Error:
    def err_to_completed_res(error):
        err_msg = f"Failed to send messsages to viber channel {cmd.channel_id.to_value_with_checksum()}: {error}"
        return CompletedWith.Error(err_msg)
    res = await send_to_viber_channel_workflow(viber_api_config, cmd)
    completed_res = res\
        .map(CompletedWith.Data)\
        .map_error(err_to_completed_res)\
        .merge()
    return completed_res
