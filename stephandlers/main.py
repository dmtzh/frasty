# import asyncio
from shared.pipeline.types import StepData
from stepdefinitions.requesturl import RequestUrl, RequestUrlInputData

from config import app, step_handler
import requesturl.handler as requesturlhandler

# ------------------------------------------------------------------------------------------------------------

@step_handler(RequestUrl, RequestUrlInputData.from_dict)
def handle_request_url_command(step_data: StepData[None, RequestUrlInputData]):
    cmd = requesturlhandler.RequestUrlCommand(step_data.data)
    return requesturlhandler.handle(cmd)

# ------------------------------------------------------------------------------------------------------------

# if __name__ == "__main__":
#     asyncio.run(app.run())