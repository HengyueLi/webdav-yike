# -*- coding: utf-8 -*-
# (c) 2009-2022 Martin Wendt and contributors; see WsgiDAV https://github.com/mar10/wsgidav
# Original PyFileServer (c) 2005 Ho Chun Wei.
# Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php


import time
import tempfile
import logging
import random
from abc import abstractmethod

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


class NoSQL:
    def __init__(self):
        # NoSQL 类型, in each table,
        self.tables = {}

    def isTableExist(self, table):
        return table in self.tables

    def createTableIfNotExist(self, table):
        if table not in self.tables:
            self.tables[table] = {}

    def dropTableIfExist(self, table):
        if self.isTableExist(table):
            del self.tables[table]

    def deleteItemIfExist(self, table, key):
        del self.tables[table][key]

    def setValue(self, table, key, value):  # overwrite
        self.tables[table][key] = value

    def setValueIfKeyNotExist(self, table, key, value):
        if key not in self.tables[table]:
            self.tables[table][key] = value

    def getValueElseNone(self, table, key):
        return self.tables[table].get(key, None)


class PathCache:
    # 测试发现文件名自动保持唯一，可作为唯一标识符

    def __init__(self, AlbumTypes):
        self.nosql = NoSQL()
        # self.AlbumTypes = AlbumTypes

        DirTypes = AlbumTypes
        self.nosql.createTableIfNotExist(table="Item")
        self.nosql.createTableIfNotExist(table="itemNameToID")
        for Dir in DirTypes:
            self.nosql.createTableIfNotExist(table=Dir)  # store info
            # self.nosql.createTableIfNotExists(table = Dir+"_list" ) #

    def cacheItem(self, item):
        self.cache_apiObj(TypeMarker="Item", apiObj=item)
        self.nosql.setValue(
            table="itemNameToID", key=item.getName(), value=item.getID()
        )

    def appendItemIntoAAlbum(self, DirType, ID, itemID, checkTableExist=True):
        table = DirType + "_list_" + ID
        if checkTableExist:
            if not self.nosql.isTableExist(table):
                self.nosql.createTableIfNotExist(table)
                self.nosql.setValue(table=table, key="0", value="0")
        Len = int(self.nosql.getValueElseNone(table=table, key="0"))
        self.nosql.setValue(table=table, key=str(Len + 1), value=itemID)
        self.nosql.setValue(table=table, key="0", value=str(Len + 1))

    def getItemListInAAlbum(self, DirType, ID):
        table = DirType + "_list_" + ID
        if self.nosql.isTableExist(table):
            Len = int(self.nosql.getValueElseNone(table=table, key="0"))
            return [
                self.nosql.getValueElseNone(table=table, key=str(i))
                for i in range(1, Len + 1)
            ]
        else:
            return None

    def cache_apiObj(self, TypeMarker, apiObj):
        table = TypeMarker
        self.nosql.setValueIfKeyNotExist(
            table=table, key=apiObj.getID(), value=apiObj.getInfo()
        )

    def getapiObjInfo(self, TypeMarker, ID):
        table = TypeMarker
        return self.nosql.getValueElseNone(table=table, key=ID)

    def getItemInfo(self, itemID):
        return self.getapiObjInfo(TypeMarker="Item", ID=itemID)

    def getItemIDByName(self, name):
        table = "itemNameToID"
        return self.nosql.getValueElseNone(table=table, key=name)

    def deleteAAlbumIfExist(self, DirType, ID):
        table1 = DirType
        table2 = DirType + "_list_" + ID
        self.nosql.deleteItemIfExist(table=table1, key=ID)
        self.nosql.dropTableIfExist(table=table2)


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
    # /All/fileName
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

    def delete(self):
        res = self.item.delete()

    def handle_delete(self):
        _logger.debug(f"handle_delete...")
        self.delete()
        return True


class onlineItemInAAlbum(onlineItem):
    # /TypeMarker/dirName/fileName
    def __init__(self, path, environ, item, AbsAlbumType, aalb):
        super().__init__(path=path, environ=environ, item=item)
        self.alb = aalb
        self.AbsAlbumType = AbsAlbumType

    def delete(self):
        if self.AbsAlbumType == "Album":
            res = self.alb.deleteItem(
                items=self.item,
                isOrigin=self.provider.config["ALBUM_ITEM_DELETE_WITH_ORIGIN"],
            )
            logging.debug(res)
        else:
            pass


class Dir_root(DAVCollection):
    def __init__(self, path, environ):
        self.provider = environ["wsgidav.provider"]
        super().__init__(path, environ)

    def get_member_names(self):
        """Return list of (direct) collection member names (UTF-8 byte strings).
        This method MUST be implemented.
        """
        return [self.provider.get_AllDirName()] + self.provider.getAlbumTypes()


class Dir_All(DAVCollection):
    def __init__(self, path, environ):
        self.provider = environ["wsgidav.provider"]
        super().__init__(path, environ)

    def get_member_names(self):
        names = []
        items = self.provider.api.get_self_All(
            typeName="Item", max=self.provider.config["ITEM_NUM_MAX_IN_DIR"]
        )
        for item in items:
            self.provider.pathCache.cacheItem(item)
            names.append(item.getName())
        return names


class Dir_TypeMarker_s(DAVCollection):
    # /TypeMarker
    def __init__(self, path, environ, TypeMarker):
        self.provider = environ["wsgidav.provider"]
        self.path = path
        self.TypeMarker = TypeMarker
        super().__init__(path, environ)

    def get_AbsAlbum_List(self):
        MaxDir = self.provider.config["ABSALUM_MAX_IN_DIR"]
        List = self.provider.api.get_self_All(typeName=self.TypeMarker, max=MaxDir)
        if List is None:
            return []
        else:
            return List
        # if self.TypeMarker == "Album":
        #     return self.provider.api.getAlbumList_All()
        # elif self.TypeMarker == "Person":
        #     return self.provider.api.getAllPersonList()
        # else:
        #     logging.error("undefined AbsAlbum type = {}".format(self.TypeMarker))

    def get_member_names(self):
        delimiter = self.provider.getDelimiter()
        albList = self.get_AbsAlbum_List()
        names = []
        for alb in albList:
            self.provider.pathCache.cache_apiObj(TypeMarker=self.TypeMarker, apiObj=alb)
            showName = Dir_Alum_Abstract.getShownNameByObj(self.provider, alb)
            names.append(showName)
        return names

    def create_collection(self, name):
        # create new alb
        assert "/" not in name
        assert self.TypeMarker == "Album"
        assert self.provider.getDelimiter() not in name
        alb = self.provider.api.createNewAlbum(Name=name)
        self.provider.pathCache.cache_apiObj(TypeMarker=self.TypeMarker, apiObj=alb)
        shownName = Dir_Alum_Abstract.getShownNameByObj(self.provider, alb)
        return self.provider.get_resource_inst(
            self.path + shownName + "/", self.environ
        )


class Dir_Alum_Abstract(DAVCollection):
    # /TypeMarker/dirName
    def __init__(self, path, environ, TypeMarker, apiObj):
        self.TypeMarker = TypeMarker
        self.provider = environ["wsgidav.provider"]
        self.apiObj = apiObj
        self.path = path
        self.environ = environ
        super().__init__(path, environ)

    @staticmethod
    def getShownNameByObj(provider, apiObj):
        dirName = apiObj.getName() + provider.getDelimiter() + apiObj.getID()
        return dirName

    @staticmethod
    def getIDByShownName(provider, shownName):
        delimiter = provider.getDelimiter()
        if delimiter not in shownName:
            logging.warning("cannot pharse id from shownName = {}".format(shownName))
            return
        else:
            return shownName.split(delimiter)[-1]

    @staticmethod
    def cacheItemsInSelfDir_byRequest(provider, TypeMarker, apiObj):
        maxNum = provider.config["ITEM_NUM_MAX_IN_" + TypeMarker.upper()]
        items = apiObj.get_sub_All(max=maxNum)
        for item in items:
            provider.pathCache.cacheItem(item=item)
        return items

    def get_member_names(self):
        maxNum = self.provider.config["ITEM_NUM_MAX_IN_" + self.TypeMarker.upper()]
        l = self.provider.pathCache.getItemListInAAlbum(
            DirType=self.TypeMarker, ID=self.apiObj.getID()
        )
        if l is None:
            items = self.cacheItemsInSelfDir_byRequest(
                provider=self.provider, TypeMarker=self.TypeMarker, apiObj=self.apiObj
            )
            # items = self.apiObj.get_sub_All(max=maxNum)
            # for item in items:
            #     self.provider.pathCache.cacheItem(item=item)
        else:
            items = []
            for itemID in l:
                info = self.provider.pathCache.getItemInfo(itemID=itemID)
                if info is None:
                    logging.error(
                        "need to request item by itemID and insert into cache"
                    )
                else:
                    item = self.provider.api.getOnlineItem_ByInfo(info=info)
                items.append(item)
        names = [item.getName() for item in items]
        return names

    def handle_move(self, dest_path):
        # 只用来重命名，不改变位置
        assert self.provider.getDelimiter() not in dest_path
        selfpath = self.path if self.path[-1] != "/" else self.path[:-1]
        destpath = dest_path if dest_path[-1] != "/" else dest_path[:-1]
        selfpaths = selfpath.split("/")
        destpaths = destpath.split("/")
        assert len(selfpaths) == len(destpaths)
        oldName = selfpaths[-1]
        newName = destpaths[-1]
        if newName != oldName:
            self.apiObj.rename(newName)
        return True

    def delete(self):
        assert self.TypeMarker == "Album"
        self.apiObj.delete(isWithItems=self.provider.config["ALBUM_DELETE_WITHITEM"])
        self.provider.pathCache.deleteAAlbumIfExist(
            DirType="Album", ID=self.apiObj.getID()
        )

    def handle_delete(self):
        assert self.TypeMarker == "Album"
        _logger.debug(f"handle_delete...")
        self.delete()
        return True

    def create_empty_resource(self, name):

        assert self.TypeMarker == "Album"

        def fun(item, api):
            alb = self.apiObj  # self.provider.getAlumb_byCacheOrRequest(ID=self.albID)
            alb.append(item)
            self.provider.pathCache.cacheItem(item)
            # self.provider.pathCache.setAlbumList(albID=self.albID, itemID=item.getID())
            self.provider.pathCache.appendItemIntoAAlbum(
                DirType=self.TypeMarker,
                ID=alb.getID(),
                itemID=item.getID(),
                checkTableExist=True,
            )

        return onlineItem_New(
            path=pathjoin(self.path, name), environ=self.environ, func_endUpload=fun
        )

    def get_display_name(self) -> str:
        # if self.TypeMarker in ["Album","Location"]:
        #     return self.apiObj.getName()
        # else:
        #     return self.name
        objName = self.apiObj.getName()
        if len(objName) == 0:
            return self.name
        else:
            return objName


class baiduphoto(DAVProvider):
    def __init__(self, config, api):
        super().__init__()
        self.pathCache = PathCache(AlbumTypes=self.getAlbumTypes())
        self.config = config
        self.api = api

    def getDelimiter(self):
        return self.config["DELIMITER"]

    def getAlbumTypes(self):
        return [
            "Album",
            "Person",
            "Location",
            "Thing",
        ]

        # self.api.get_self_All(typeName="",max=)

    def get_apiObj_byCacheOrRequest(self, TypeMarker, ID):
        # fun_ListAll = {
        #     "Item": self.api.getAllItems,
        #     "Album": self.api.getAlbumList_All,
        #     "Person": self.api.getAllPersonList,
        # }.get(TypeMarker, None)
        fun_ListAll = lambda: self.api.get_self_All(typeName=TypeMarker)

        fun_Req = {
            "Album": self.api.getAlbum_ByID,
        }.get(TypeMarker, None)
        # --- load info into object
        # InfoloadFunc = {
        #     "Item": self.api.getOnlineItem_ByInfo,
        #     "Album": self.api.getAlbum_ByInfo,
        #     "Person": self.api.getPerson_ByInfo,
        # }.get(TypeMarker, None)
        InfoloadFunc = lambda info: self.api.loadSelfByInfo(
            typeName=TypeMarker, info=info
        )
        # --------------------------------------------------
        info = self.pathCache.getapiObjInfo(TypeMarker=TypeMarker, ID=ID)
        if info is None:
            logging.debug("apiObj [{},{}] not in cache".format(TypeMarker, ID))
            if fun_Req is not None:
                logging.debug("Viable to request info to get apiObj")
                apiObj = fun_Req(ID=ID)
                self.pathCache.cache_apiObj(TypeMarker=TypeMarker, apiObj=apiObj)
                return apiObj
            else:
                logging.warning(
                    "need to implement function to request [{}] info by ID".format(
                        TypeMarker
                    )
                )
                logging.warning(
                    "As a compromise, load all [{}] info. This would be slow!".format(
                        TypeMarker
                    )
                )
                apiObjs = fun_ListAll()
                for o in apiObjs:
                    self.pathCache.cache_apiObj(TypeMarker=TypeMarker, apiObj=o)
                info = self.pathCache.getapiObjInfo(TypeMarker=TypeMarker, ID=ID)
                if info is None:
                    logging.error(
                        "({},{}) is not found. Consider increase NUM_MAX. Or it does not exist at all".format(
                            TypeMarker, ID
                        )
                    )
                    return None
        if InfoloadFunc is None:
            logging.error("TypeMarker = {} not support in get_apiObj_byCacheOrRequest")
        return InfoloadFunc(info)

    # def getItem_byNameWithCache(self, Name , paths):
    #     ID = self.pathCache.getItemIDByName(Name)
    #     if ID is None:
    #         logging.warning("need to implement request id by filename")
    #         logging.warning("As a compromise, reload all items, this is slow!")
    #         items = self.api.getAllItems()
    #         for item in items:
    #             self.pathCache.cache_apiObj(TypeMarker="Item", apiObj=item)
    #         ID = self.pathCache.getItemIDByName(Name)
    #         if ID is None:
    #             logging.error(
    #                 "item name {} not found. Consider increasing ITEM_NUM_MAX_IN_DIR?"
    #             )
    #             return None
    #     return self.getItem_byCache(ID)
    def getItem_byNameWithCache(self, Name, paths):
        ID = self.pathCache.getItemIDByName(Name)
        if ID is None:
            if paths[0] == self.get_AllDirName():
                return None
            if hasattr(self, "requestItemByfileName"):
                item = self.requestItemByfileName(Name)
                self.pathCache.cache_apiObj(TypeMarker="Item", apiObj=item)
                return item
            else:
                logging.warning(
                    "need to implement request id by filename, path=[{}]".format(paths)
                )
                if paths[0] == self.get_AllDirName():
                    logging.warning("As a compromise, reload all items, this is slow!")
                    items = self.api.getAllItems(max=self.config["ITEM_NUM_MAX_IN_DIR"])
                    for item in items:
                        self.pathCache.cache_apiObj(TypeMarker="Item", apiObj=item)
                    ID = self.pathCache.getItemIDByName(Name)
                    if ID is None:
                        logging.error(
                            "item name {} not found. Consider increasing ITEM_NUM_MAX_IN_DIR or set it as -1".format(
                                name
                            )
                        )
                        return None
                    else:
                        return self.getItem_byCache(ID)
                else:  # /TypeMarker/dirName/fileName
                    aalbID = Dir_Alum_Abstract.getIDByShownName(
                        provider=self, shownName=paths[1]
                    )
                    aalb = self.get_apiObj_byCacheOrRequest(
                        TypeMarker=paths[0], ID=aalbID
                    )
                    Null = Dir_Alum_Abstract.cacheItemsInSelfDir_byRequest(
                        provider=self, TypeMarker=paths[0], apiObj=aalb
                    )
                    ID = self.pathCache.getItemIDByName(Name)
                    if ID is None:
                        logging.error(
                            "cannot load item information. Considering cd to root dir".format()
                        )
                        return None
                    else:
                        return self.getItem_byCache(ID)
        else:
            return self.getItem_byCache(ID)

    # ====================================================
    # best to implement
    #
    # def requestItemByID(self,ID):
    #     # return item

    # def requestItemByfileName(self,fileName):
    #     # return item
    # ====================================================

    def getItem_byCache(self, ID):
        info = self.pathCache.getItemInfo(itemID=ID)
        if info is None:
            logging.warning("not in cache, No way to obtain it")
        else:
            return self.api.getOnlineItem_ByInfo(info=info)

    def get_AllDirName(self):
        maxNum = int(self.config["ITEM_NUM_MAX_IN_DIR"])
        if maxNum <= 0:
            return "All"
        else:
            return "All(latest{})".format(maxNum)

    @staticmethod
    def matchFileNamePrefix(fileName):
        blackList = [
            ".DS_Store",  # macOS folder meta data
            "Thumbs.db",  # Windows image previews
            "._",  # macOS hidden data files
        ]
        for b in blackList:
            l = len(b)
            if fileName[:l] == b:
                return True
        return False

    def get_resource_inst(self, path, environ):
        """Return info dictionary for path.

        See get_resource_inst()
        """

        paths = path.strip("/").split("/")
        delimiter = self.getDelimiter()
        DirAllName = self.get_AllDirName()

        logging.debug("DAV path=" + "{} {}".format(path, paths))

        ########################################################
        #           filter
        ########################################################
        if self.matchFileNamePrefix(paths[-1]):
            return None

        if path == "/":
            return Dir_root(path=path, environ=environ)

        ########################################################
        #           /All
        ########################################################
        if path in ["/{}".format(DirAllName), "/{}/".format(DirAllName)]:
            return Dir_All(path=path, environ=environ)

        if paths[0] == DirAllName and paths[1] != "":  # /All/filename.sufix
            fileName = paths[1]
            item = self.getItem_byNameWithCache(Name=fileName, paths=paths)
            return onlineItem(path=path, environ=environ, item=item)

        ########################################################
        #           /AbstractAlbum
        #
        # path = /TypeMarker
        #      = /TypeMarker/AlbShownName
        #      = /TypeMarker/AlbShownName/fileName
        ########################################################
        if paths[0] not in self.getAlbumTypes():
            return None

        if len(paths) == 1:
            return Dir_TypeMarker_s(path=path, environ=environ, TypeMarker=paths[0])
        elif len(paths) == 2:
            ID = Dir_Alum_Abstract.getIDByShownName(provider=self, shownName=paths[1])
            if ID is None:
                return None
            aalb = self.get_apiObj_byCacheOrRequest(TypeMarker=paths[0], ID=ID)
            if aalb is None:
                return
            else:
                return Dir_Alum_Abstract(
                    path=path, environ=environ, TypeMarker=paths[0], apiObj=aalb
                )
        elif len(paths) == 3:
            fileName = paths[2]
            item = self.getItem_byNameWithCache(Name=fileName, paths=paths)
            if item is None:
                return None
            else:
                ID = Dir_Alum_Abstract.getIDByShownName(
                    provider=self, shownName=paths[1]
                )
                aalb = self.get_apiObj_byCacheOrRequest(TypeMarker=paths[0], ID=ID)
                return onlineItemInAAlbum(
                    path=path,
                    environ=environ,
                    item=item,
                    AbsAlbumType=paths[0],
                    aalb=aalb,
                )
        else:
            logging.error("not implemented")

        print("if end error")
        return None
