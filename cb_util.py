#!/usr/bin/env python


def tag_user_data(value):
    '''Adds tags to user data so that it can be redacted later'''
    return '<ud>' + str(value) + '</ud>'
