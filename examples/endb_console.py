#!/usr/bin/env python3

import cmd
import endb
import pprint
import urllib.error
import re
import time

class EndbConsole(cmd.Cmd):
    def __init__(self, url, accept='application/ld+json', username=None, password=None, prompt='-> '):
        super().__init__()
        self.url = url
        self.accept = accept
        self.username = username
        self.password = password
        self.prompt = prompt
        self.timer = False

    def emptyline(self):
        pass

    def complete_timer(self, text, line, begidx, endidx):
        return [x for x in ['on', 'off'] if x.startswith(text)]

    def do_timer(self, arg):
        'Sets or shows timer.'
        if arg:
            self.timer = arg == 'on'
        if self.timer:
            print('on')
        else:
            print('off')

    def complete_accept(self, text, line, begidx, endidx):
        return [x for x in ['application/json', 'application/ld+json', 'text/csv', 'application/vnd.apache.arrow.file'] if x.startswith(text)]

    def do_accept(self, arg):
        'Sets or shows the accepted mime type.'
        if arg:
            self.accept = re.sub('=\s*', '', arg)
        print(self.accept)

    def do_url(self, arg):
        'Sets or shows the database URL.'
        if arg:
            self.url = re.sub('=\s*', '', arg)
        print(self.url)

    def do_username(self, arg):
        'Sets or shows the database user.'
        if arg:
            self.username = re.sub('=\s*', '', arg)
        print(self.username)

    def do_password(self, arg):
        'Sets the database password.'
        if arg:
            self.password = re.sub('=\s*', '', arg)
        if self.password is not None:
            print('*******')
        else:
            print(None)

    def do_quit(self, arg):
        'Quits the console.'
        return 'stop'

    def default(self, line):
        start_time = None
        if line == 'EOF':
            return 'stop'
        try:
            start_time = time.time()
            result = endb.Endb(self.url, self.accept, self.username, self.password).sql(line)
            if self.accept == 'text/csv':
                print(result.strip())
            elif self.accept == 'application/vnd.apache.arrow.file':
                print(result)
            else:
                pprint.pprint(result)
        except urllib.error.HTTPError as e:
            print('%s %s' % (e.code, e.reason))
            body = e.read().decode()
            if body:
                print(body)
        except urllib.error.URLError as e:
            print(self.url)
            print(e.reason)
        if start_time and self.timer:
            print('Elapsed: %f ms' % (time.time() - start_time))

if __name__ == '__main__':
    import sys
    url = 'http://localhost:3803/sql'
    if len(sys.argv) > 1:
        url = sys.argv[1]
    prompt = '-> '
    if not sys.stdin.isatty():
        prompt = ''
    try:
        EndbConsole(url, prompt=prompt).cmdloop()
        if sys.stdin.isatty():
            print()
    except KeyboardInterrupt:
        print()
