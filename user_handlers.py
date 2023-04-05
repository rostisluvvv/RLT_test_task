import json
import logging
from aiogram import Dispatcher
from aiogram.types import Message

from lexicon import LEXICON
from service import get_aggregated_values

logger = logging.getLogger(__name__)


async def process_start_command(message: Message):
    await message.answer(LEXICON[message.text])


async def process_aggregated_values(message: Message):
    try:
        await message.answer(get_aggregated_values(json.loads(message.text)))
    except (ValueError, TypeError) as error:
        logger.info(error)
        await message.answer(text=LEXICON['invalid_request'])


def register_user_handlers(dp: Dispatcher):
    dp.register_message_handler(process_start_command, commands=['start'])
    dp.register_message_handler(process_aggregated_values)
