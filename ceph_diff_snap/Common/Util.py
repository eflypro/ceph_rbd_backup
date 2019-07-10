# -*- coding:utf-8 -*-

import os
import random
import shutil
import uuid
import md5


class Util:
    """
    实用工具类
    """

    def __init__(self):
        pass

    @staticmethod
    def generate_mac():
        """
        创建MAC地址
        :return:
        """
        mac_list = []
        for i in range(1, 7):
            rand_str = "".join(random.sample("0123456789abcdef", 2))
            # 保证第一段MAC的第二个16进制字符为2,6,a,e
            if i == 1:
                again = True
                while again:
                    if rand_str[1] == "2" or rand_str[1] == "6" or rand_str[1] == "a" or rand_str[1] == "e":
                        again = False
                    else:
                        rand_str = "".join(random.sample("0123456789abcdef", 2))
                        again = True
                # while
            # if
            mac_list.append(rand_str)
        # for
        rand_mac = ":".join(mac_list)
        return rand_mac
    # def

    @staticmethod
    def get_uuid():
        """ This function return a uuid."""
        md = md5.new()
        md.update(str(uuid.uuid1()))
        uuid_md5 = md.hexdigest()
        new_uuid = "%s-%s-%s-%s-%s" % (uuid_md5[:8], uuid_md5[8:12], uuid_md5[12:16], uuid_md5[16:20], uuid_md5[20:])
        return new_uuid
    # def

    @staticmethod
    def exist_file(str_path):
        return os.path.isfile(str_path)

    @staticmethod
    def exist_dir(str_path):
        return os.path.isdir(str_path)

    @staticmethod
    def mkdir_p(path, mask=0o777):
        if not os.path.exists(path):
            os.makedirs(path, mask)

    @staticmethod
    def rmdirs(path):
        if os.path.exists(path):
            shutil.rmtree(path)

# class
