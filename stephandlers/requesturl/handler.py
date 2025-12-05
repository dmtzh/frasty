import asyncio
from dataclasses import dataclass

import aiohttp

from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error
from shared.utils.asyncresult import async_ex_to_error_result
from stepdefinitions.requesturl import RequestUrlInputData
from stepdefinitions.shared import HttpResponseData

@dataclass(frozen=True)
class RequestUrlCommand:
    data: RequestUrlInputData

@async_ex_to_error_result(Error.from_exception)
async def request_data(request_url: RequestUrlInputData) -> CompletedResult:
    async with aiohttp.ClientSession() as session:
        timeout_15_seconds = aiohttp.ClientTimeout(total=15)
        try:
            async with session.request(method=request_url.http_method, url=request_url.url, headers=request_url.headers, timeout=timeout_15_seconds) as response:
                # bytes = await response.read()
                # json = await response.json()
                # content_stream = response.content
                content = await response.text()
                response_data = HttpResponseData(response.status, response.content_type, content)
                response_data_dict = HttpResponseData.to_dict(response_data)
                return CompletedWith.Data(response_data_dict)
        except asyncio.TimeoutError:
            return CompletedWith.Error(f"Request timeout {timeout_15_seconds.total} seconds")
        except aiohttp.client_exceptions.ClientConnectorError:
            return CompletedWith.Error(f"Cannot connect to {request_url.url} ({request_url.http_method})")

async def handle(cmd: RequestUrlCommand):
    res = await request_data(cmd.data)
    return res.default_with(lambda err: CompletedWith.Error(f"Request url unexpected error {err.message}"))