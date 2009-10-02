#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Connector/Python, native MySQL driver written in Python.
Copyright 2009 Sun Microsystems, Inc. All rights reserved. Use is subject to license terms.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
"""

import sys

from distutils.core import setup
from mysql.django._version import version as mysql_django_version

_name = 'Django Database Backend using MySQL Connector/Python'
_version = '%d.%d.%d' % mysql_django_version[0:3]
_packages = ['mysql','mysql.django']
    
setup(
    name = _name,
    version = _version,
    author = 'Geert Vanderkelen',
    author_email = 'geert.vanderkelen@sun.com',
    url = 'http://dev.mysql.com/usingmysql/python/',
    download_url = 'http://dev.mysql.com/downloads/connector/python/',
    packages = _packages
)
