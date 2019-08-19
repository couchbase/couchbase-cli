#!/usr/bin/env python3


def tag_user_data(value):
    '''Adds tags to user data so that it can be redacted later'''
    return f'<ud>{str(value)}</ud>'
