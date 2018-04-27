#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
This module provides utilities that helps the logging of the application.
"""

import functools
import traceback
import sys

    
def bot_retry_on_exceptions(exceptions = []):
    if exceptions == []:
        raise Exception('Must provide list of exceptions to handle')
    def outer_wrap(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            # raise exception if no TRIES member is defined
            if not hasattr(self, 'BOT_RETRIES'):
                raise Exception('Expected to find BOT_RETRIES member')
            for i in range(self.BOT_RETRIES + 1):
                try:
                    result = f(self, *args, **kwargs)
                    return result
                except Exception as e:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    for exception in exceptions:
                        if isinstance(e, exception):
                            break
                    else:
                        # Encoutered exception that is not in the handled exceptions list
                        self.take_screenshot()

                        # Log exception message and stacktrace
                        logger.warning('unhandled exception encoutered %s' % exc_value)
                        
                        for line in traceback.format_tb(exc_traceback):
                            logger.warning(line.strip())

                        # Give up
                        raise

                    # Log exception message and stacktrace
                    logger.warning('controlled exception encoutered %s' % exc_value)
                    
                    for line in traceback.format_tb(exc_traceback):
                        logger.warning(line.strip())

                    # Give up after i retries
                    if i == self.BOT_RETRIES:
                        logger.warning('Giving up on controlled exception after {} tries'.format(i))
                        raise

                    # Fallthrough to reset bot and retry
                    self.reset_bot()
        return wrapper
    return outer_wrap
