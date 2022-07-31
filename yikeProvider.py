# -*- coding: utf-8 -*-
# (c) 2009-2022 Martin Wendt and contributors; see WsgiDAV https://github.com/mar10/wsgidav
# Original PyFileServer (c) 2005 Ho Chun Wei.
# Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php


import time
import tempfile
import logging
import random


import sys, os, io


from wsgidav import util
from wsgidav.dav_error import (
    HTTP_FORBIDDEN,
    DAVError,
    PRECONDITION_CODE_ProtectedProperty,
)
from wsgidav.dav_provider import (
    DAVProvider,
    _DAVResource,
    DAVCollection,
    DAVNonCollection,
)
from wsgidav.stream_tools import StreamingFile, FileLikeQueue


__docformat__ = "reStructuredText"

_logger = util.get_module_logger(__name__)


def pathjoin(path1, path2):
    if path1[-1] == "/":
        path1 = path1[:-1]
    if path2[0] == "/":
        path2 = path2[1:]
    return path1 + "/" + path2


class PathCache:
    # 测试发现文件名自动保持唯一，可作为唯一标识符

    def __init__(self):
        self.cache = {
            "items": {},
            "albums": {},
            # "itemInfo":{},
            "AlbumItemList": {},
            "itemNameToID": {},
        }

    def setAlbumList(self, albID, itemID):
        if albID not in self.cache["AlbumItemList"]:
            self.cache["AlbumItemList"][albID] = []
        if itemID not in self.cache["AlbumItemList"][albID]:
            self.cache["AlbumItemList"][albID].append(itemID)

    def cacheItem(self, item):
        itemID = item.getID()
        if itemID not in self.cache["items"]:
            self.cache["items"][itemID] = dict(item.getInfo())
            self.cache["itemNameToID"][item.getName()] = item.getID()

    def cacheAlbum(self, album):
        ID = album.getID()
        if ID not in self.cache["albums"]:
            self.cache["albums"][ID] = dict(album.getInfo())

    def getItemInfo(self, itemID):
        return self.cache["items"].get(itemID, None)

    def getAlbumInfo(self, albID):
        return self.cache["albums"].get(albID, None)

    def getAlbumItemList(self, albID):
        return self.cache["AlbumItemList"].get(albID, None)

    def getItemIDByName(self, name):
        return self.cache["itemNameToID"].get(name, None)

    def setAlbumItemList(self, albID, itemIDList):
        self.cache["AlbumItemList"][albID] = list(itemIDList)

    def deleteAlbumIfExist(self, albID):
        if albID in self.cache["albums"]:
            del self.cache["albums"][albID]
        if albID in self.cache["AlbumItemList"]:
            del self.cache["AlbumItemList"][albID]


class onlineItem_New(DAVNonCollection):
    def __init__(self, path, environ, func_endUpload):
        # def func_endUpload(item,api)
        super().__init__(path, environ)
        self.provider = environ["wsgidav.provider"]
        self.api = self.provider.api
        fileName = path.split("/")[-1]
        fileName = self.name_append_UID(fileName=fileName)
        self.tmpFilePath = os.path.join(tempfile.gettempdir(), fileName)
        self.endFunc = func_endUpload

    def getUID(self):
        n = 1000
        return str(int(time.time() * n) + random.randint(0, n - 1))

    def name_append_UID(self, fileName):
        name, sufix = fileName.split(".")
        return name + "_" + self.getUID() + "." + sufix

    def get_content_length(self):
        return 0

    def get_content(self):
        return b"0"

    def get_etag(self):
        return None

    def support_etag(self):
        return False

    def begin_write(self, *, content_type=None):
        return open(self.tmpFilePath, "wb")

    def end_write(self, *, with_errors):
        """Called when PUT has finished writing.
        This is only a notification. that MAY be handled.
        """
        newitem = self.api.upload_1file(filePath=self.tmpFilePath)
        os.remove(self.tmpFilePath)
        self.endFunc(newitem, self.api)


class onlineItem(DAVNonCollection):
    def __init__(self, path, environ, item):
        self.provider = environ["wsgidav.provider"]
        super().__init__(path, environ)
        # self.environ = environ
        self.APIitem = item
        self.item = item

    def get_content_length(self):
        """Returns the byte length of the content.
        MUST be implemented.
        See also _DAVResource.get_content_length()
        """
        return self.APIitem.getSize()

    def get_content(self):
        """Open content as a stream for reading.
        Returns a file-like object / stream containing the contents of the
        resource specified.
        The application will close() the stream.
        This method MUST be implemented by all providers.
        """
        filestream = io.BytesIO()
        filestream.write(self.APIitem.getContent_byRequest())
        filestream.seek(0)  # ???
        return filestream

    def get_creation_date(self):
        return self.item.getCreationDate()

    def get_last_modified(self):
        return self.item.getModificationDate()

    def get_etag(self):
        """
        See http://www.webdav.org/specs/rfc4918.html#PROPERTY_getetag
        This method SHOULD be implemented, especially by non-collections.
        Return None if not supported for this resource instance.
        See also `DAVNonCollection.support_etag()` and `util.get_file_etag(path)`.
        """
        return None  #  I do not know what is it

    def support_etag(self):
        """Return True, if this resource supports ETags.
        See also `DAVNonCollection.get_etag()`.
        """
        return False

    @staticmethod
    def getIDByShownName(provider, shownName):
        ID = provider.pathCache.getItemIDByName(shownName)
        if ID is None:
            logging.warning(
                "cannot map showName [{}] to ID, cache missing".format(shownName)
            )
        else:
            return ID

    def delete(self):
        res = self.item.delete()

    def handle_delete(self):
        _logger.debug(f"handle_delete...")
        self.delete()
        return True


class onlineItemInAlbum(onlineItem):
    def __init__(self, path, environ, item, album):
        super().__init__(path=path, environ=environ, item=item)
        self.alb = album

    def delete(self):
        res = self.alb.deleteItem(
            items=self.item,
            isOrigin=self.provider.config["ALBUM_ITEM_DELETE_WITH_ORIGIN"],
        )
        logging.debug(res)

    def handle_delete(self):
        _logger.debug(f"handle_delete...")
        self.delete()
        return True


class Dir_root(DAVCollection):
    def __init__(self, path, environ):
        self.provider = environ["wsgidav.provider"]
        super().__init__(path, environ)

    def get_member_names(self):
        """Return list of (direct) collection member names (UTF-8 byte strings).
        This method MUST be implemented.
        """
        return [
            "Albums",
            self.provider.get_AllDirName(),  # All (latest x)
        ]


class Dir_Album(DAVCollection):
    def __init__(self, path, environ, dirName):
        self.provider = environ["wsgidav.provider"]
        self.path = path
        super().__init__(path, environ)
        delimiter = self.provider.getDelimiter()
        if delimiter in dirName:
            self.albID = self.getIDByShownName(self.provider, dirName)
            self.album = self.provider.getAlumb_byCacheOrRequest(ID=self.albID)
            self.provider.pathCache.cacheAlbum(self.album)
            self.isinit = True
        else:
            self.isinit = False
            logging.error("cannot pharse album ID from path")

    @staticmethod
    def getShownNameByID(provider, album):
        alb = album
        dirName = alb.getName() + provider.getDelimiter() + alb.getID()
        return dirName

    @staticmethod
    def getIDByShownName(provider, shownName):
        delimiter = provider.getDelimiter()
        if delimiter not in shownName:
            logging.warning("cannot pharse id from albname = {}".format(shownName))
            return
        else:
            return shownName.split(delimiter)[-1]

    def get_member_names(self):
        if not self.isinit:
            return []
        names = []
        l = self.provider.pathCache.getAlbumItemList(albID=self.albID)
        if l is None:
            items = self.album.get_AllOnlineItems(
                max=self.provider.config["ITEM_NUM_MAX_IN_ALBUM"]
            )
            for item in items:
                self.provider.pathCache.cacheItem(item=item)
        else:
            items = []
            for itemID in l:
                info = self.provider.pathCache.getItemInfo(itemID=itemID)
                if info is None:
                    pass
                    logging.error(
                        "need to request item by itemID and insert into cache"
                    )
                else:
                    item = self.provider.api.getOnlineItem_ByInfo(info=info)
                items.append(item)
        names = [item.getName() for item in items]
        return names

    def delete(self):
        alb = self.provider.getAlumb_byCacheOrRequest(ID=self.albID)
        alb.delete(isWithItems=self.provider.config["ALBUM_DELETE_WITHITEM"])
        self.provider.pathCache.deleteAlbumIfExist(albID=self.albID)

    def handle_delete(self):
        _logger.debug(f"handle_delete...")
        self.delete()
        return True

    def create_empty_resource(self, name):
        def fun(item, api):
            alb = self.provider.getAlumb_byCacheOrRequest(ID=self.albID)
            alb.append(item)
            self.provider.pathCache.cacheItem(item)
            self.provider.pathCache.setAlbumList(albID=self.albID, itemID=item.getID())

        return onlineItem_New(
            path=pathjoin(self.path, name), environ=self.environ, func_endUpload=fun
        )

    def get_display_name(self) -> str:
        return self.album.getName()

    def handle_move(self, dest_path):
        selfpath = self.path if self.path[-1] != "/" else self.path[:-1]
        destpath = dest_path if dest_path[-1] != "/" else dest_path[:-1]
        selfpaths = selfpath.split("/")
        destpaths = destpath.split("/")
        assert len(selfpaths) == len(destpaths)
        oldName = selfpaths[-1]
        newName = destpaths[-1]
        if newName != oldName:
            self.album.rename(newName)
        return True


class Dir_All(DAVCollection):
    def __init__(self, path, environ):
        self.provider = environ["wsgidav.provider"]
        super().__init__(path, environ)

    def get_member_names(self):
        names = []
        items = self.provider.api.getAllItems(
            max=self.provider.config["ITEM_NUM_MAX_IN_DIR"]
        )
        for item in items:
            self.provider.pathCache.cacheItem(item)
            names.append(item.getName())
        return names


class Dir_Albums(DAVCollection):
    # /Albums

    def __init__(self, path, environ):
        self.provider = environ["wsgidav.provider"]
        self.path = path
        super().__init__(path, environ)

    def get_member_names(self):
        names = []
        delimiter = self.provider.getDelimiter()
        albs = self.provider.api.getAlbumList_All()
        for alb in albs:
            self.provider.pathCache.cacheAlbum(alb)
            dirName = alb.getName() + delimiter + alb.getID()
            names.append(dirName)
        return names

    def create_collection(self, name):
        # create new alb
        assert "/" not in name
        alb = self.provider.api.createNewAlbum(Name=name)
        shownName = Dir_Album.getShownNameByID(self.provider, alb)
        return self.provider.get_resource_inst(
            self.path + shownName + "/", self.environ
        )


class baiduphoto(DAVProvider):
    def __init__(self, config, api):
        super().__init__()
        self.pathCache = PathCache()
        self.config = config
        self.api = api

    def getDelimiter(self):
        return self.config["DELIMITER"]

    def getAlumb_byCacheOrRequest(self, ID):
        info = self.pathCache.getAlbumInfo(albID=ID)
        if info is None:
            alb = self.api.getAlbum_ByID(ID=ID)
            self.pathCache.cacheAlbum(alb)
            return alb
        else:
            return self.api.getAlbum_ByInfo(info=info)

    def getItem_byNameWithCache(self, Name):
        ID = self.pathCache.getItemIDByName(Name)
        if ID is None:
            logging.warning("need to implement request id by filename")
        else:
            return ID

    def getItem_byCache(self, ID):
        info = self.pathCache.getItemInfo(itemID=ID)
        if info is None:
            logging.warning("not in cache, No way to obtain it")
        else:
            return self.api.getOnlineItem_ByInfo(info=info)

    def get_AllDirName(self):
        return "All(latest{})".format(int(self.config["ITEM_NUM_MAX_IN_DIR"]))

    def get_resource_inst(self, path, environ):
        """Return info dictionary for path.

        See get_resource_inst()
        """
        paths = path.split("/")[1:]
        delimiter = self.getDelimiter()
        DirAllName = self.get_AllDirName()

        logging.debug("DAV path=", path, paths)

        if path == "/":
            return Dir_root(path=path, environ=environ)

        ########################################################
        #           /All
        ########################################################
        if path in ["/{}".format(DirAllName), "/{}/".format(DirAllName)]:
            return Dir_All(path=path, environ=environ)

        if paths[0] == DirAllName and paths[1] != "":
            itemName = paths[1]
            itemID = self.getItem_byNameWithCache(itemName)
            item = self.getItem_byCache(itemID)
            return onlineItem(path=path, environ=environ, item=item)

        ########################################################
        #           /Albums
        ########################################################
        if path in ["/Albums", "/Albums/"]:
            return Dir_Albums(path=path, environ=environ)

        if paths[0] == "Albums" and paths[1] != "":  # /Albums/*
            albShowName = paths[1]  # <albName_D_ID>
            albID = Dir_Album.getIDByShownName(provider=self, shownName=albShowName)
            if albID is None:
                return None
            else:
                alb = self.getAlumb_byCacheOrRequest(ID=albID)
            if len(paths) == 2:  # /Albums/<albName_D_ID>
                return Dir_Album(path=path, environ=environ, dirName=albShowName)
            elif paths[2] == "":  # /Albums/<albName_D_ID>/
                if delimiter not in paths[1]:
                    return None
                else:
                    return Dir_Album(path=path, environ=environ, dirName=albShowName)
            elif paths[2] != "":  # /Albums/<albName_D_ID>/<itemName.sufix>
                itemID = onlineItem.getIDByShownName(self, paths[2])
                if itemID is None:
                    return None
                else:
                    item = self.getItem_byCache(ID=itemID)
                    return onlineItemInAlbum(
                        path=path, environ=environ, item=item, album=alb
                    )

        # elif path=="/All":
        # items = self.api.getAllItems()
        # itemNames = []
        # for item in items:
        #     logging.debug(len(items))
        #     self.pathCache.cacheItem(item)
        #     itemNames.append( item.getID()+ "." + item.getName().split(".")[-1]   )
        # return Directory(subNameList=itemNames,path=path,environ=environ)
        #     return Directory(subNameList=["empty"],path=path,environ=environ)
        # elif path == "/Albums":
        #     print("here")
        #     albs = self.api.getAlbumList()['items']
        #     return Directory(subNameList=[alb.getName() for alb in albs],path=path,environ=environ)
        # elif path == "/Albums/":
        #     print("here")
        #     albs = self.api.getAlbumList()['items']
        #     return Directory(subNameList=[alb.getName() for alb in albs],path=path,environ=environ)
        else:
            print("else error")
        print("if end error")
        return None
