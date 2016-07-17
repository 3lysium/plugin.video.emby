# -*- coding: utf-8 -*-

#################################################################################################

import sqlite3
import threading

import xbmc

#################################################################################################

KODI = xbmc.getInfoLabel('System.BuildVersion')[:2]

#################################################################################################


def video_database():
        
    db_version = {

        '13': 78, # Gotham
        '14': 90, # Helix
        '15': 93, # Isengard
        '16': 99, # Jarvis
        '17': 104 # Krypton
    }

    path = xbmc.translatePath("special://database/MyVideos%s.db"
        % db_version.get(KODI, "")).decode('utf-8')

    return path

def music_database():
    
    db_version = {

        '13': 46, # Gotham
        '14': 48, # Helix
        '15': 52, # Isengard
        '16': 56, # Jarvis
        '17': 60  # Krypton
    }

    path = xbmc.translatePath("special://database/MyMusic%s.db"
        % db_version.get(KODI, "")).decode('utf-8')

    return path


class DatabaseConn(object):
    # To be called as context manager - with statement

    def __init__(self, database_file="video", commit_mode=None):
        """
        database_file can be custom. See self._SQL below
        commit_mode is set to None for isolation_level. See python documentation.
        """
        self.db_file = database_file
        self.commit_mode = commit_mode

    def __enter__(self):
        # Open the connection
        self.path = self._SQL(self.db_file)

        self.conn = sqlite3.connect(self.path, isolation_level=self.commit_mode, timeout=20)
        return self.conn

    def _SQL(self, media_type):
        # media_type: emby, texture, music, video, custom like :memory: or path
        if media_type == "emby":
            return xbmc.translatePath("special://database/emby.db").decode('utf-8')
        elif media_type == "texture":
            return xbmc.translatePath("special://database/Textures13.db").decode('utf-8')
        elif media_type == "music":
            return music_database()
        elif media_type == "video":
            return video_database()
        else: # custom path
            return self.db_file

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Close the connection
        self.conn.close()

        if exc_type is not None:
            # Errors were raised
            # log("Error type %s value %s traceback %s" % (exc_type, exc_val, exc_tb))
            pass

def query(query, connection=None, conn_type=None, *args):
    # For queries that don't return results
    if connection is None:
        if conn_type is None:
            return False
        else:
            connection = DatabaseConn(conn_type)

    attempts = 3
    while True:
        try:
            with connection as conn:
                # raise sqlite3.OperationalError("database is locked")
                if not args:
                    return conn.execute(query)
                elif isinstance(args[0], tuple):
                    # Multiple entries for the same query
                    return conn.executemany(query, args)
                else:
                    return conn.execute(query, args)
            
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                # Database is locked, retry
                attempts -= 1
                xbmc.sleep(1000)
            else:
                raise

        if not attempts:
            return False