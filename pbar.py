"""Couchbase CLI progress bar utilities"""

import platform
import struct
import sys
import time

if platform.system() == "Windows":
    import ctypes
    from ctypes import c_long, c_ulong
    gHandle = ctypes.windll.kernel32.GetStdHandle(c_long(-11))

    def get_terminal_width():
        """Get windows terminal size on standard windows terminal"""
        try:
            from ctypes import windll, create_string_buffer
            std_handle = windll.kernel32.GetStdHandle(-12)
            csbi = create_string_buffer(22)
            res = windll.kernel32.GetConsoleScreenBufferInfo(std_handle, csbi)
            if res:
                (_, _, cur_x, cur_y, _, left, top, right, bottom, _, _) =\
                    struct.unpack("hhhhHhhhhhh", csbi.raw)
                size_x = right - left
                size_y = bottom - top
                return size_x, size_y, cur_x, cur_y
        except:
            pass

        return None, None, None, None

    def bold(text):
        """Makes a string bold in the terminal"""
        return text

    def move_cursor_relative_y(rows):
        """Moves the terminal cursor the specified number of rows"""
        if rows == 0:
            return

        _, _, cur_x, cur_y = get_terminal_width()

        coord_y = 0
        if cur_y - rows > 0:
            coord_y = cur_y - rows

        coord = (cur_x + (coord_y << 16))
        ctypes.windll.kernel32.SetConsoleCursorPosition(gHandle, c_ulong(coord))

    def move_cursor_absolute_x(cols):
        """Moves the terminal cursor absolute column position"""
        _, _, cur_x, cur_y = get_terminal_width()
        if cur_x == cols:
            return

        coord = (cols + (cur_y << 16))
        ctypes.windll.kernel32.SetConsoleCursorPosition(gHandle, c_ulong(coord))

elif platform.system() in ['Linux', 'Darwin'] or platform.system().startswith('CYGWIN'):
    import fcntl
    import termios
    import os

    def get_terminal_width():
        """Get linux/osx terminal size on standard linux/osx terminal"""
        def ioctl_gwinsz(fd):
            """Gets the windows size for a given file descriptor"""
            try:
                return struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ, '1234'))
            except:
                return None

        window_size = ioctl_gwinsz(0) or ioctl_gwinsz(1) or ioctl_gwinsz(2)
        if window_size is None:
            try:
                fd = os.open(os.ctermid(), os.O_RDONLY)
                window_size = ioctl_gwinsz(fd)
                os.close(fd)
            except:
                return None, None, None, None

        return int(window_size[1]), int(window_size[0]), 0, 0

    def bold(text):
        """Makes a string bold in the terminal"""
        return "\033[1m" + text + "\033[0m"

    def move_cursor_relative_y(rows):
        """Moves the terminal cursor the specified number of rows"""
        if rows == 0:
            return
        elif rows > 0:
            sys.stdout.write(("\033[%dA" % rows))
        else:
            sys.stdout.write(("\033[%dA" % (rows*-1)))

    def move_cursor_absolute_x(cols):
        """Moves the terminal cursor absolute column position"""
        sys.stdout.write("\r")
        if cols > 0:
            sys.stdout.write("\033[%dC" % cols)

else:
    def get_terminal_width():
        """Make the terminal size undefined since we don't know what os this is"""
        return None, None, None, None

    def bold(text):
        """Makes a string bold in the terminal"""
        return text

    def move_cursor_relative_y(rows):
        """Moves the terminal cursor the specified number of rows"""
        pass

    def move_cursor_absolute_x(cols):
        """Moves the terminal cursor absolute column position"""
        pass

class TopologyProgressBar(object):
    """Generates a progress bar for topology change progress such as failover and rebalances"""

    def __init__(self, rest_client, type, hidden=False):
        self.rest_client = rest_client
        self.term_width, _, _, _ = get_terminal_width()
        self.hidden = hidden
        self.type = type

    def show(self):
        """Shows the rebalance progress bar"""

        if not self.hidden:
            if self.term_width is None:
                self.hidden = True
                sys.stdout.write("Unable to display progress bar on this os\n")
            elif self.term_width < 80:
                self.hidden = True
                sys.stdout.write("Unable to display progress bar, terminal with must be at " +
                                 "least 80 columns\n")

        status, errors = self.rest_client.rebalance_status()
        if errors:
            return errors

        if status["status"] == 'notRunning':
            return None

        cur_bucket = 0
        total_buckets = 0
        cur_bucket_name = "no_bucket"

        if not self.hidden:
            self._report_progress(0, cur_bucket, total_buckets, cur_bucket_name, 0)

        while status["status"] in['running', 'unknown']:
            if status["status"] == 'unknown' or self.hidden:
                time.sleep(1)
            else:
                cur_bucket = status["details"]["curBucket"]
                total_buckets = status["details"]["totalBuckets"]
                cur_bucket_name = status["details"]["curBucketName"]
                move_cursor_relative_y(3)
                move_cursor_absolute_x(0)
                self._report_progress(status["details"]["progress"],
                                      status["details"]["curBucket"],
                                      status["details"]["totalBuckets"],
                                      status["details"]["curBucketName"],
                                      status["details"]["docsRemaining"])
                time.sleep(status["details"]["refresh"])

            status, errors = self.rest_client.rebalance_status()
            if errors:
                return errors

        if status["status"] == 'notRunning' and not self.hidden:
            move_cursor_relative_y(3)
            move_cursor_absolute_x(0)
            self._report_progress(100, cur_bucket, total_buckets, cur_bucket_name, 0)
        elif status["status"] == 'errored':
            return [status["msg"]]

        return None

    def _report_progress(self, perc_complete, cur_bucket, total_buckets, bucket_name, remaining):
        bar_size = self.term_width - 10
        bars = int(perc_complete/100.0 * (bar_size))
        spaces = int(bar_size - bars)

        cur_buckets_str = str(cur_bucket)
        total_buckets_str = str(total_buckets)
        if cur_bucket < 10:
            cur_buckets_str = "0" + str(cur_bucket)
        if total_buckets < 10:
            total_buckets_str = "0" + str(total_buckets)
        d_count_str = f'Bucket: {cur_buckets_str}/{total_buckets_str} '
        d_remain_str = f' {remaining!s} docs remaining'

        d_name_size = (self.term_width - len(d_remain_str) - len(d_count_str))
        if len(bucket_name) > (d_name_size - 2):
            d_name_str = f'({bucket_name[0:d_name_size - 5]}...)'
        else:
            d_name_str = f'({bucket_name }){" "* (d_name_size - len(bucket_name) - 2)}'

        sys.stdout.write(f'{self.type}{" " * (self.term_width - len(self.type))}\n')
        sys.stdout.write(f'{d_count_str}{d_name_str}{d_remain_str}\n')
        sys.stdout.write(f"[{'='* bars}{' ' * spaces}] {bold(f'{perc_complete:.2f}')}%\n")
