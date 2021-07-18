#!/usr/bin/env python

"""
这个脚本的作用是把数据发送到 Kafaka
"""


class SendKafkaMiddleware:

    def send(self, value, *args, **kwargs):
        print(value)
        pass
