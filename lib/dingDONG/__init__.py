# (c) 2017-2021, Tal Shany <tal.shany@biSkilled.com>
#
# This file is part of dingDONG
#
# dingDong is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# dingDong is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with dingDong.  If not, see <http://www.gnu.org/licenses/>.

from dingDONG.bl.dd import dingDONG as dingDONG
from dingDONG.config import config  as Config

from dingDONG.misc.enums import eConn

UPDATE  = eConn.updateMethod.UPDATE
DROP    = eConn.updateMethod.DROP
NO_UPDATE=eConn.updateMethod.NO_UPDATE


