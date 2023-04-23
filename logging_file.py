import logging

logging.basicConfig(level=logging.DEBUG,
                    format="{asctime} {levelname:<8} {message}",
                    style='{',
                    filename='steeleyelog.log',
                    filemode='a')

username='saakshi'

log=logging.getLogger()
