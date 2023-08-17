#!/usr/bin/env python3

import cmd
import endb
import pprint
import urllib.error

class EndbShell(cmd.Cmd):
    intro = 'Endatabas is a SQL document database with full history.'
    prompt = '-> '
    file = None

    def __init__(self, url, accept='application/ld+json', username=None, password=None):
        super().__init__()
        self.url = url
        self.accept = accept
        self.username = username
        self.password = password

    def emptyline(self):
        pass

    def complete__accept(self, text, line, begidx, endidx):
        return [x for x in ['application/json', 'application/ld+json', 'text/csv'] if x.startswith(text)]

    def do__accept(self, arg):
        'Accepted mime type'
        if arg:
            self.accept = arg
        print(self.accept)

    def do__url(self, arg):
        'Database URL'
        if arg:
            self.url = arg
        print(self.url)

    def do__username(self, arg):
        'Database user'
        if arg:
            self.username = arg
        print(self.username)

    def do__password(self, arg):
        'Database password'
        if arg:
            self.password = arg
        print('*******')

    def default(self, line):
        try:
            auth = None
            if self.username and self.password:
                auth = (self.username, self.password)
            result = endb.sql(line, accept=self.accept, url=self.url, auth=auth)
            if self.accept == 'text/csv':
                print(result.strip())
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

if __name__ == "__main__":
    import sys
    url = 'http://localhost:3803/sql'
    if len(sys.argv) > 1:
        url = sys.argv[1]
    EndbShell(url).cmdloop()
