#!/usr/local/bin/python
#coding=utf-8

"""
calc edit distance/similarity between two strings based on unicode
distance is Levenshtein Edit Distance
similarity = 100 - (distance * 100 / max(len(string_left), len(string_right)))
"""
import chardet
import logging

#import Pycode as pycode 

ADD_COST = 1
DEL_COST = 1
REPLACE_COST = 1

class EditDistanceError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class EditDistance(object):
    """
    use dp, with space requirement len(string_left)*len(string_right) to calc edit distance
    you can specify cost for each operation: ADD, DEL, REPLACE
    TODO: use linear space 
    """
    @staticmethod
    def Calc(string_left, string_right, 
            add_cost = ADD_COST, 
            del_cost = DEL_COST, 
            replace_cost = REPLACE_COST):

        string_left = string_left.lower()
        string_right = string_right.lower()

        #before decode, convert to utf-8 first...
        u_string_left = to_unicode(string_left)
        u_string_right = to_unicode(string_right)

        if not u_string_right and not u_string_left:
            logging.warn("EditDistance, two strings are all null")
            return (0, 100, 100)

        string_left_len = len(u_string_left)
        string_right_len = len(u_string_right)

        if string_left_len * string_right_len > 1024 * 1024:
            raise EditDistanceError("too long string, with: %d, %d" % (
                string_left_len, string_right_len))

        #now increase string len by 1 to faciliate dp calc
        max_len = max(string_left_len, string_right_len)
        string_left_len += 1
        string_right_len += 1
        dists = [0] * (string_left_len * string_right_len)

        for i in xrange(string_left_len):
            dists[i * string_right_len] = i * add_cost

        for j in xrange(string_right_len):
            dists[j] = j * del_cost

        for i in xrange(1, string_left_len):
            for j in xrange(1, string_right_len):
                add_dist = dists[(i - 1) * string_right_len + j] + add_cost
                del_dist = dists[i * string_right_len + (j - 1)] + del_cost
                _replace_cost = 0 if u_string_left[i-1] == u_string_right[j-1] else replace_cost
                replace_dist = dists[(i - 1) * string_right_len + (j - 1)] + _replace_cost
                dists[i * string_right_len + j] = min(add_dist, del_dist, replace_dist)

        distance = dists[-1]
        similarity = 100 - distance * 100 / max_len
        return (similarity, distance, max_len)           

def to_unicode(instr):
    """
    把字符串转换为unicode编码
    """
    if isinstance(instr, unicode):
        return instr

    # 若是不能识别字符串编码，则返回为空字符串
    # result中有置信度
    result = chardet.detect(instr)
    if result:
        code_stype = result['encoding']
        return instr.decode(code_stype, 'ignore')
    logging.warn("EditDistance, instr error:%s" % instr)
    return u''

def test():
    """
    """
    test_cases = [
            #string_left, string_right, distance, similarity
            ("abcdef", "abcef", 1, 84), 
            ("中国", "中华人民共和国", 5, 29),
            ("美团", "美团", 0, 100),
            ("美团", "中国", 2, 0),
            ("【鲤城区】仅售138元！市场价168元的元和1916-创逸会馆的自助晚餐一人次！拉手券消费时间为18:00-21:00！缤纷美味大餐，随心所欲搭配，你的美食世界你做主！", 
                "【新门街】仅售48元！市场价58元的元和1916-创逸会馆的自助午餐一人次！拉手券消费时间：周一至周五11:30-13:30！缤纷美味大餐，随心所欲搭配，你的美食世界你做主！", 19, 79)
            ]
    fail_cnt = 0
    for test_case in test_cases:
        string_left, string_right, distance, similarity = test_case
        print "try to get similarity for: %s, %s" % (string_left, string_right)
        _simi, _dist, _max_len = EditDistance.Calc(string_left, string_right)
        if _simi != similarity or _dist != distance:
            print "failed for: %s vs %s, with expect dist: %d, calc dist: %d" % (
                    string_left, string_right, distance, _dist)
            fail_cnt += 1
        else:
            print "pass, with dist: %d, similarity: %s" % (_dist, _simi)

    if fail_cnt == 0:
        print "congratulations....all passed"
    else:
        print "poor boy, %d cases failed" % fail_cnt

if __name__ == "__main__":
    """
    """
    test()
