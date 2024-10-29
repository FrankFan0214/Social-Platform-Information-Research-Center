# -*- coding: utf-8 -*-
"""project2_當日心情.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1u-Wa6Kp8GnSexfgvsK-PZfkgA4EH8w2I

### ***Install & Import***
"""

!pip install pymongo
!pip install mysql-connector-python
!pip install jieba
!pip install pandas
!pip install nltk
!pip install pandas      # 對應於 dplyr
!pip install textblob  # 對應於 tidytext
!pip install ckiptagger # 中研院的結巴pro

import os
import pandas as pd
import pymongo
import mysql.connector
import jieba
import re
import pandas as pd       # 對應於 dplyr
import numpy as np        # 常用於資料處理
import re                 # 對應於 stringr 的功能
import jieba              # 對應於 jiebaR，專門用於中文分詞
import textblob           # 對應於 tidytext 的一些功能
import json               # 用於處理JSON資料
import matplotlib.font_manager
from pymongo import MongoClient
from mysql.connector import Error
from collections import Counter, defaultdict
from datetime import datetime

"""### ***Emotion Dictionary***"""

# 定義情緒詞典
EMOTION_DICT = {
    # 喜悅
    '開心': '喜悅', '快樂': '喜悅', '高興': '喜悅', '興奮': '喜悅',
    '愉快': '喜悅', '歡喜': '喜悅', '開朗': '喜悅', '暢快': '喜悅',
    '歡樂': '喜悅', '笑': '喜悅', '微笑': '喜悅', '大笑': '喜悅',
    '欣喜': '喜悅', '欣慰': '喜悅', '喜悅': '喜悅', '喜氣洋洋': '喜悅',
    '眉開眼笑': '喜悅', '樂呵呵': '喜悅', '樂滋滋': '喜悅', '喜形於色': '喜悅',
    '心花怒放': '喜悅', '春風得意': '喜悅', '得意洋洋': '喜悅', '歡天喜地': '喜悅',
    '興高采烈': '喜悅', '喜氣盈門': '喜悅', '笑逐顏開': '喜悅', '笑容滿面': '喜悅',
    '喜笑顏開': '喜悅', '欣欣然': '喜悅', '雀躍': '喜悅', '飛揚': '喜悅',
    '歡欣': '喜悅', '歡快': '喜悅', '愉悅': '喜悅', '喜不自勝': '喜悅',
    '欣喜若狂': '喜悅', '樂不可支': '喜悅', '笑嘻嘻': '喜悅', '眉飛色舞': '喜悅',
    '心曠神怡': '喜悅', '舒暢': '喜悅', '愉快': '喜悅', '歡愉': '喜悅',
    '喜氣': '喜悅', '喜慶': '喜悅', '歡暢': '喜悅', '怡然': '喜悅',


    # 安心
    '放心': '安心', '安心': '安心', '踏實': '安心', '安穩': '安心',
    '平安': '安心', '安定': '安心', '安寧': '安心', '鎮定': '安心',
    '穩重': '安心', '沉著': '安心', '泰然': '安心', '安詳': '安心',
    '安泰': '安心', '安然': '安心', '安適': '安心', '安逸': '安心',
    '安祥': '安心', '安和': '安心', '安樂': '安心', '安靜': '安心',
    '安分': '安心', '安居': '安心', '安度': '安心', '安妥': '安心',
    '安恬': '安心', '安好': '安心', '安康': '安心', '安全': '安心',
    '安生': '安心', '安身': '安心', '安土': '安心', '安養': '安心',
    '安泊': '安心', '安置': '安心', '安眠': '安心', '安歇': '安心',
    '安枕': '安心', '安然無恙': '安心', '安之若素': '安心', '安身立命': '安心',
    '安營扎寨': '安心', '安營紮寨': '安心', '安步當車': '安心', '安居樂業': '安心',
    '安堵': '安心', '安頓': '安心', '安排': '安心', '安息': '安心',


    # 期待
    '期待': '期待', '期盼': '期待', '盼望': '期待', '希望': '期待',
    '憧憬': '期待', '展望': '期待', '嚮往': '期待', '企盼': '期待',
    '冀望': '期待', '期許': '期待', '期望': '期待', '渴望': '期待',
    '企望': '期待', '盼': '期待', '盼念': '期待', '守望': '期待',
    '期求': '期待', '期念': '期待', '翹首': '期待', '翹盼': '期待',
    '翹企': '期待', '翹望': '期待', '瞻望': '期待', '盼顧': '期待',
    '盼佇': '期待', '期凝': '期待', '期盼': '期待', '期待': '期待',
    '望眼欲穿': '期待', '翹首以待': '期待', '引領期盼': '期待', '盼星星盼月亮': '期待',
    '望穿秋水': '期待', '殷切期待': '期待', '引頸期待': '期待', '期期艾艾': '期待',
    '期頤': '期待', '期勉': '期待', '期約': '期待', '期待': '期待',
    '盼顧': '期待', '盼望': '期待', '盼歸': '期待', '盼著': '期待',


    # 驚訝
    '驚訝': '驚訝', '震驚': '驚訝', '吃驚': '驚訝', '詫異': '驚訝',
    '意外': '驚訝', '驚奇': '驚訝', '驚詫': '驚訝', '愕然': '驚訝',
    '驚愕': '驚訝', '驚異': '驚訝', '驚歎': '驚訝', '讚歎': '驚訝',
    '驚嘆': '驚訝', '驚恐': '驚訝', '驚懼': '驚訝', '驚慌': '驚訝',
    '驚惶': '驚訝', '驚惶失措': '驚訝', '驚慌失措': '驚訝', '大吃一驚': '驚訝',
    '目瞪口呆': '驚訝', '瞠目結舌': '驚訝', '駭然': '驚訝', '震駭': '驚訝',
    '愣住': '驚訝', '怔住': '驚訝', '呆住': '驚訝', '嚇': '驚訝',
    '嚇一跳': '驚訝', '錯愕': '驚訝', '震悚': '驚訝', '咋舌': '驚訝',
    '詫': '驚訝', '駭': '驚訝', '愣': '驚訝', '怔': '驚訝',
    '嘖嘖稱奇': '驚訝', '嘖嘖稱異': '驚訝', '驚為天人': '驚訝', '瞪目結舌': '驚訝',
    '瞠目': '驚訝', '愣神': '驚訝', '發愣': '驚訝', '發怔': '驚訝',


    # 生氣
    '生氣': '生氣', '憤怒': '生氣', '火大': '生氣', '惱火': '生氣',
    '暴怒': '生氣', '氣憤': '生氣', '惱怒': '生氣', '發飆': '生氣',
    '動怒': '生氣', '發怒': '生氣', '發火': '生氣', '惱羞': '生氣',
    '光火': '生氣', '氣沖沖': '生氣', '怒氣沖天': '生氣', '怒髮衝冠': '生氣',
    '氣急敗壞': '生氣', '氣憤填膺': '生氣', '怒不可遏': '生氣', '暴跳如雷': '生氣',
    '火冒三丈': '生氣', '大發雷霆': '生氣', '怒氣衝衝': '生氣', '氣惱': '生氣',
    '惱': '生氣', '怒': '生氣', '憤': '生氣', '怨': '生氣',
    '怨恨': '生氣', '憎': '生氣', '憤恨': '生氣', '憤憤': '生氣',
    '憤然': '生氣', '憤慨': '生氣', '憤懣': '生氣', '憎恨': '生氣',
    '氣炸': '生氣', '氣瘋': '生氣', '氣死': '生氣', '氣壞': '生氣',
    '氣急': '生氣', '氣極': '生氣', '氣憋': '生氣', '氣悶': '生氣',


    # 討厭
    '討厭': '討厭', '厭惡': '討厭', '煩': '討厭', '嫌惡': '討厭',
    '反感': '討厭', '憎惡': '討厭', '厭煩': '討厭', '嫌棄': '討厭',
    '厭倦': '討厭', '厭棄': '討厭', '討人厭': '討厭', '嫌': '討厭',
    '嫌棄': '討厭', '嫌惡': '討厭', '嫌厭': '討厭', '討嫌': '討厭',
    '煩躁': '討厭', '煩悶': '討厭', '煩心': '討厭', '煩惱': '討厭',
    '厭世': '討厭', '厭煩': '討厭', '厭惡': '討厭', '厭棄': '討厭',
    '厭憎': '討厭', '厭倦': '討厭', '厭膩': '討厭', '厭棄': '討厭',
    '憎恨': '討厭', '憎惡': '討厭', '憎嫌': '討厭', '憎厭': '討厭',
    '反胃': '討厭', '反感': '討厭', '反對': '討厭', '反惡': '討厭',
    '嫌棄': '討厭', '嫌厭': '討厭', '嫌惡': '討厭', '嫌憎': '討厭',
    '討厭': '討厭', '討人嫌': '討厭', '討人厭': '討厭', '討不喜': '討厭',


    # 難過
    '難過': '難過', '傷心': '難過', '悲傷': '難過', '哀傷': '難過',
    '悲痛': '難過', '痛心': '難過', '哭泣': '難過', '心疼': '難過',
    '悲': '難過', '哀': '難過', '愁': '難過', '憂': '難過',
    '苦': '難過', '慟': '難過', '痛': '難過', '傷': '難過',
    '悲哀': '難過', '悲痛': '難過', '悲慘': '難過', '悲涼': '難過',
    '悲戚': '難過', '悲憤': '難過', '悲憫': '難過', '悲泣': '難過',
    '悲切': '難過', '悲咽': '難過', '悲愴': '難過', '悲嘆': '難過',
    '悲鳴': '難過', '悲慟': '難過', '悲怨': '難過', '悲苦': '難過',
    '痛哭': '難過', '痛苦': '難過', '痛楚': '難過', '痛悔': '難過',
    '心酸': '難過', '心痛': '難過', '心碎': '難過', '心傷': '難過',
    '黯然': '難過', '黯淡': '難過', '慘然': '難過', '慘澹': '難過',


    # 焦慮
    '焦慮': '焦慮', '擔心': '焦慮', '憂慮': '焦慮', '不安': '焦慮',
    '緊張': '焦慮', '慌張': '焦慮', '惶恐': '焦慮', '憂心': '焦慮',
    '忐忑': '焦慮', '惴惴': '焦慮', '煎熬': '焦慮', '憂懼': '焦慮',
    '憂愁': '焦慮', '憂悶': '焦慮', '憂心忡忡': '焦慮', '憂心如焚': '焦慮',
    '忐忑不安': '焦慮', '坐立不安': '焦慮', '心神不寧': '焦慮', '心慌意亂': '焦慮',
    '心急如焚': '焦慮',

    # 正面
    '支持': '正面', '自由': '正面', '希望': '正面', '喜歡': '正面',
    '相信': '正面', '不錯': '正面', '同意': '正面', '嘻嘻': '正面',
    '贊同': '正面', '認可': '正面', '肯定': '正面', '讚賞': '正面',
    '欣賞': '正面', '贊成': '正面', '認同': '正面', '讚同': '正面',
    '支援': '正面', '支撐': '正面', '支援': '正面', '支持者': '正面',
    '擁護': '正面', '維護': '正面', '擁戴': '正面', '贊助': '正面',
    '自在': '正面', '自主': '正面', '自如': '正面', '自由自在': '正面',
    '解放': '正面', '開放': '正面', '無拘無束': '正面', '暢快': '正面',
    '嚮往': '正面', '期望': '正面', '期待': '正面', '冀望': '正面',
    '摯愛': '正面', '喜愛': '正面', '鍾愛': '正面', '愛慕': '正面',
    '篤信': '正面', '深信': '正面', '信任': '正面', '信賴': '正面',
    '優良': '正面', '良好': '正面', '優質': '正面', '出色': '正面',


    # 反感
    '噁心': '反感', '討厭': '反感', '不爽': '反感', '垃圾': '反感',
    '低能': '反感', '智障': '反感', '白痴': '反感', '白癡': '反感',
    '噁': '反感', '嫌惡': '反感', '厭惡': '反感', '反胃': '反感',
    '作嘔': '反感', '惡心': '反感', '嘔心': '反感', '嫌棄': '反感',
    '厭煩': '反感', '煩躁': '反感', '厭倦': '反感', '厭棄': '反感',
    '痛恨': '反感', '憎恨': '反感', '憎惡': '反感', '憎嫌': '反感',
    '反感': '反感', '反對': '反感', '抗議': '反感', '抵制': '反感',
    '嫌棄': '反感', '嫌厭': '反感', '嫌惡': '反感', '嫌憎': '反感',
    '排斥': '反感', '排擠': '反感', '排除': '反感', '驅逐': '反感',
    '唾棄': '反感', '唾罵': '反感', '咒罵': '反感', '辱罵': '反感',

    # 失望
    '可憐': '失望', '可悲': '失望', '不好': '失望', '失敗': '失望',
    '擔心': '失望', '隨便': '失望', '沮喪': '失望', '灰心': '失望',
    '喪氣': '失望', '泄氣': '失望', '氣餒': '失望', '頹喪': '失望',
    '失意': '失望', '消沉': '失望', '低落': '失望', '頹廢': '失望',
    '失落': '失望', '挫敗': '失望', '挫折': '失望', '打擊': '失望',
    '墮落': '失望', '沉淪': '失望', '沉淪': '失望', '萎靡': '失望',
    '衰敗': '失望', '衰退': '失望', '衰落': '失望', '衰微': '失望',
    '頹唐': '失望', '頹廢': '失望', '頹靡': '失望', '頹勢': '失望',
    '失望': '失望', '失意': '失望', '失落': '失望', '失志': '失望',
    '悲觀': '失望', '消極': '失望', '頹喪': '失望', '低迷': '失望',

    # 違規
#   '違規': '違規', '違法': '違規', '問題': '違規', '八卦': '違規',
#   '犯規': '違規', '違反': '違規', '觸法': '違規', '犯法': '違規',
#   '越軌': '違規', '違紀': '違規', '違章': '違規', '違例': '違規',
#   '違禁': '違規', '違約': '違規', '違背': '違規', '違逆': '違規',
#   '違抗': '違規', '違令': '違規', '違命': '違規', '違制': '違規',
#   '違犯': '違規', '違反': '違規', '違背': '違規', '違逆': '違規',
#   '違拗': '違規', '違悖': '違規', '違忤': '違規', '違諫': '違規',
#   '犯禁': '違規', '犯戒': '違規', '犯例': '違規', '犯規': '違規',
#   '越權': '違規', '越位': '違規', '越矩': '違規', '越界': '違規',
#   '觸犯': '違規', '觸法': '違規', '觸例': '違規', '觸規': '違規',

    # 貶低
    '白癡': '貶低', '智障': '貶低', '低能': '貶低', '白痴': '貶低',
    '笨蛋': '貶低', '蠢貨': '貶低', '廢物': '貶低', '無能': '貶低',
    '腦殘': '貶低', '蠢材': '貶低', '呆子': '貶低', '傻瓜': '貶低',
    '笨豬': '貶低', '蠢豬': '貶低', '笨蛋': '貶低', '笨拙': '貶低',
    '愚笨': '貶低', '愚蠢': '貶低', '愚昧': '貶低', '愚鈍': '貶低',
    '愚钝': '貶低', '愚魯': '貶低', '愚痴': '貶低', '愚懵': '貶低',
    '遲鈍': '貶低', '魯鈍': '貶低', '糊塗': '貶低', '癡呆': '貶低',
    '無知': '貶低', '無腦': '貶低', '沒腦': '貶低', '缺腦': '貶低',
    '蠢笨': '貶低', '蠢鈍': '貶低', '蠢拙': '貶低', '蠢態': '貶低',

    # 憤怒
    '生氣': '憤怒', '憤怒': '憤怒', '惱火': '憤怒', '暴怒': '憤怒',
    '火大': '憤怒', '發飆': '憤怒', '發火': '憤怒', '發怒': '憤怒',
    '氣憤': '憤怒', '氣惱': '憤怒', '氣炸': '憤怒', '氣瘋': '憤怒',
    '暴跳': '憤怒', '狂怒': '憤怒', '大怒': '憤怒', '震怒': '憤怒',
    '怒不可遏': '憤怒', '怒髮衝冠': '憤怒', '怒火中燒': '憤怒', '怒氣沖天': '憤怒',
    '火冒三丈': '憤怒', '大發雷霆': '憤怒', '雷霆之怒': '憤怒', '暴跳如雷': '憤怒',
    '氣急敗壞': '憤怒', '氣憤填膺': '憤怒', '怒形於色': '憤怒', '怒氣衝衝': '憤怒',
    '憤憤不平': '憤怒', '憤慨': '憤怒', '憤恨': '憤怒', '憤然': '憤怒',

    # 厭惡
    '討厭': '厭惡', '噁心': '厭惡', '厭煩': '厭惡', '厭惡': '厭惡',
    '嫌惡': '厭惡', '反感': '厭惡', '排斥': '厭惡', '抵制': '厭惡',
    '嫌棄': '厭惡', '嫌厭': '厭惡', '嫌憎': '厭惡', '厭棄': '厭惡',
    '憎惡': '厭惡', '憎恨': '厭惡', '痛恨': '厭惡', '可惡': '厭惡',
    '可憎': '厭惡', '可惡': '厭惡', '可厭': '厭惡', '可恨': '厭惡',
    '作嘔': '厭惡', '反胃': '厭惡', '嘔心': '厭惡', '惡心': '厭惡',
    '腥臭': '厭惡', '骯髒': '厭惡', '污穢': '厭惡', '齷齪': '厭惡',
    '骯髒': '厭惡', '污濁': '厭惡', '髒亂': '厭惡', '汙穢': '厭惡',

    # 質疑
    '質疑': '質疑', '懷疑': '質疑', '存疑': '質疑', '疑問': '質疑',
    '不信': '質疑', '狐疑': '質疑', '疑慮': '質疑', '疑竇': '質疑',
    '困惑': '質疑', '不解': '質疑', '納悶': '質疑', '費解': '質疑',
    '不明': '質疑', '不懂': '質疑', '不確定': '質疑', '不肯定': '質疑',
    '半信半疑': '質疑', '將信將疑': '質疑', '疑神疑鬼': '質疑', '疑雲': '質疑',
    '疑點': '質疑', '疑案': '質疑', '疑團': '質疑', '疑陣': '質疑',
    '不可信': '質疑', '不可靠': '質疑', '不可取': '質疑', '不可信賴': '質疑',
    '似是而非': '質疑', '模稜兩可': '質疑', '曖昧不明': '質疑', '撲朔迷離': '質疑',


    # 諷刺
    '諷刺': '諷刺', '嘲諷': '諷刺', '譏諷': '諷刺', '揶揄': '諷刺',
    '嘲笑': '諷刺', '訕笑': '諷刺', '譏笑': '諷刺', '恥笑': '諷刺',
    '冷嘲': '諷刺', '熱諷': '諷刺', '嘲弄': '諷刺', '戲弄': '諷刺',
    '譏刺': '諷刺', '譏評': '諷刺', '譏誚': '諷刺', '譏彈': '諷刺',
    '挖苦': '諷刺', '奚落': '諷刺', '嘲謔': '諷刺', '調侃': '諷刺',
    '訕笑': '諷刺', '訕謗': '諷刺', '訕評': '諷刺', '訕刺': '諷刺',
    '譏評': '諷刺', '貶損': '諷刺', '貶抑': '諷刺', '貶低': '諷刺',
    '嘲弄': '諷刺', '戲謔': '諷刺', '取笑': '諷刺', '笑話': '諷刺',

    # 不滿
    '不滿': '不滿', '不爽': '不滿', '不悅': '不滿', '不快': '不滿',
    '不服': '不滿', '不甘': '不滿', '不平': '不滿', '不願': '不滿',
    '不樂': '不滿', '不安': '不滿', '不適': '不滿', '不舒': '不滿',
    '抱怨': '不滿', '怨言': '不滿', '怨聲': '不滿', '怨氣': '不滿',
    '埋怨': '不滿', '牢騷': '不滿', '怨懟': '不滿', '怨恨': '不滿',
    '不悅': '不滿', '不樂': '不滿', '不快': '不滿', '不適': '不滿',
    '不舒服': '不滿', '不自在': '不滿', '不痛快': '不滿', '不舒坦': '不滿',
    '憤憤不平': '不滿', '憤懣': '不滿', '憤慨': '不滿', '怏怏不樂': '不滿',

    # 擔憂
    '擔心': '擔憂', '憂慮': '擔憂', '擔憂': '擔憂', '憂心': '擔憂',
    '焦慮': '擔憂', '焦急': '擔憂', '憂急': '擔憂', '憂悶': '擔憂',
    '憂愁': '擔憂', '憂傷': '擔憂', '憂鬱': '擔憂', '憂煩': '擔憂',
    '憂慮重重': '擔憂', '憂心忡忡': '擔憂', '憂心如焚': '擔憂', '憂思': '擔憂',
    '掛心': '擔憂', '掛慮': '擔憂', '牽掛': '擔憂', '介懷': '擔憂',
    '放心不下': '擔憂', '寢食難安': '擔憂', '坐立不安': '擔憂', '心神不寧': '擔憂',
    '憂愁': '擔憂', '憂悶': '擔憂', '憂傷': '擔憂', '憂鬱': '擔憂',
    '憂懼': '擔憂', '憂慮': '擔憂', '憂思': '擔憂', '憂煩': '擔憂',

    # 支持
    '支持': '支持', '贊同': '支持', '同意': '支持', '認可': '支持',
    '贊成': '支持', '支援': '支持', '擁護': '支持', '支撐': '支持',
    '支援': '支持', '支助': '支持', '支應': '支持', '支持者': '支持',
    '贊助': '支持', '贊助者': '支持', '贊同者': '支持', '支持者': '支持',
    '擁戴': '支持', '擁護': '支持', '擁立': '支持', '擁戴者': '支持',
    '認同': '支持', '認可': '支持', '認許': '支持', '認證': '支持',
    '贊許': '支持', '贊可': '支持', '贊揚': '支持', '贊美': '支持',
    '支持': '支持', '支援': '支持', '支助': '支持', '支應': '支持',

    # 後悔
    '後悔': '後悔', '懊悔': '後悔', '悔恨': '後悔', '遺憾': '後悔',
    '懊惱': '後悔', '悔不當初': '後悔', '追悔莫及': '後悔', '悔之晚矣': '後悔',
    '悔青': '後悔', '懊喪': '後悔', '自責': '後悔', '懺悔': '後悔',
    '痛悔': '後悔', '悔改': '後悔', '悔意': '後悔', '追悔': '後悔',
    '後悔莫及': '後悔', '悔不該': '後悔', '悔錯': '後悔', '悔過': '後悔',
    '悔恨交加': '後悔', '悔之不及': '後悔', '追悔末及': '後悔', '悔不絕': '後悔',
    '悔悟': '後悔', '悔恨不已': '後悔', '悔不堪言': '後悔', '追悔無及': '後悔',
    '悔之無及': '後悔', '追悔不及': '後悔', '懊惱不已': '後悔', '悔恨萬分': '後悔',
    '悔不迭': '後悔', '追悔不已': '後悔', '懊惱不堪': '後悔', '懺悔不已': '後悔',

    # 麻煩
#   '麻煩': '麻煩', '困擾': '麻煩', '棘手': '麻煩', '難辦': '麻煩',
#   '費事': '麻煩', '累贅': '麻煩', '添亂': '麻煩', '攪擾': '麻煩',
#   '勞煩': '麻煩', '為難': '麻煩', '費神': '麻煩', '費力': '麻煩',
#   '費時': '麻煩', '勞師動眾': '麻煩', '添麻煩': '麻煩', '惹麻煩': '麻煩',
#   '找麻煩': '麻煩', '製造麻煩': '麻煩', '帶來麻煩': '麻煩', '出狀況': '麻煩',
#   '出問題': '麻煩', '徒增麻煩': '麻煩', '添堵': '麻煩', '費周章': '麻煩',
#   '勞駕': '麻煩', '勞累': '麻煩', '打擾': '麻煩', '困境': '麻煩',
#   '難處': '麻煩', '艱難': '麻煩', '繁瑣': '麻煩', '複雜': '麻煩',
#   '費工夫': '麻煩', '費勁': '麻煩', '費心': '麻煩', '費功夫': '麻煩',

    # 成功
    '成功': '成功', '達成': '成功', '實現': '成功', '完成': '成功',
    '做到': '成功', '成就': '成功', '贏得': '成功', '征服': '成功',
    '突破': '成功', '得勝': '成功', '成功率': '成功', '告捷': '成功',
    '大功告成': '成功', '功德圓滿': '成功', '圓滿': '成功', '順利': '成功',
    '達標': '成功', '成效': '成功', '建樹': '成功', '創舉': '成功',
    '突破性': '成功', '里程碑': '成功', '佳績': '成功', '捷報': '成功',
    '創下紀錄': '成功', '成果': '成功', '斬獲': '成功', '勝利': '成功',
    '獲勝': '成功', '奪冠': '成功', '稱霸': '成功', '成名': '成功',
    '出人頭地': '成功', '平步青雲': '成功', '登峰造極': '成功', '揚名立萬': '成功',

    # 謝謝
    '謝謝': '謝謝', '感謝': '謝謝', '感恩': '謝謝', '多謝': '謝謝',
    '致謝': '謝謝', '謝意': '謝謝', '感激': '謝謝', '感念': '謝謝',
    '謝天謝地': '謝謝', '萬分感謝': '謝謝', '銘謝': '謝謝', '道謝': '謝謝',
    '答謝': '謝謝', '謝禮': '謝謝', '謝忱': '謝謝', '謝函': '謝謝',
    '謝卡': '謝謝', '謝詞': '謝謝', '謝信': '謝謝', '感恩戴德': '謝謝',
    '感激不盡': '謝謝', '感激涕零': '謝謝', '感謝不盡': '謝謝', '感恩圖報': '謝謝',
    '感激萬分': '謝謝', '感激不已': '謝謝', '感激莫名': '謝謝', '銘感五內': '謝謝',
    '感激之情': '謝謝', '感謝之意': '謝謝', '謝意難盡': '謝謝', '感激之至': '謝謝',
    '感恩之心': '謝謝', '感激之心': '謝謝', '感謝之心': '謝謝', '謝意難表': '謝謝',

    # 優惠
    '優惠': '優惠', '特價': '優惠', '折扣': '優惠', '促銷': '優惠',
    '減價': '優惠', '便宜': '優惠', '划算': '優惠', '優待': '優惠',
    '優勢': '優惠', '實惠': '優惠', '優惠券': '優惠', '折價券': '優惠',
    '特惠': '優惠', '優惠價': '優惠', '限時優惠': '優惠', '折扣價': '優惠',
    '特賣': '優惠', '讓利': '優惠', '折讓': '優惠', '優惠方案': '優惠',
    '優惠活動': '優惠', '折價': '優惠', '減免': '優惠', '促銷價': '優惠',
    '特價品': '優惠', '優待券': '優惠', '回饋': '優惠', '優惠卷': '優惠',
    '特惠價': '優惠', '優惠專案': '優惠', '優惠期間': '優惠', '促銷活動': '優惠',
    '打折': '優惠', '降價': '優惠', '特價優惠': '優惠', '限時特價': '優惠',

    # 有趣
    '有趣': '有趣', '有意思': '有趣', '好玩': '有趣', '有意思': '有趣',
    '逗趣': '有趣', '趣味': '有趣', '妙趣': '有趣', '風趣': '有趣',
    '幽默': '有趣', '搞笑': '有趣', '有梗': '有趣', '趣味性': '有趣',
    '饒富趣味': '有趣', '富趣味': '有趣', '趣味橫生': '有趣', '趣味盎然': '有趣',
    '饒富意趣': '有趣', '趣味十足': '有趣', '趣味無窮': '有趣', '妙趣橫生': '有趣',
    '趣味性強': '有趣', '趣味豐富': '有趣', '趣味無限': '有趣', '趣味盎然': '有趣',
    '妙不可言': '有趣', '妙趣無窮': '有趣', '趣味橫溢': '有趣', '趣味無窮': '有趣',
    '趣味盎然': '有趣', '妙趣橫生': '有趣', '趣味性強': '有趣', '趣味豐富': '有趣',
    '趣味無限': '有趣', '趣味十足': '有趣', '趣味橫生': '有趣', '趣味盎然': '有趣',

    # 願意
    '願意': '願意', '情願': '願意', '樂意': '願意', '甘願': '願意',
    '同意': '願意', '肯定': '願意', '答應': '願意', '首肯': '願意',
    '允許': '願意', '應允': '願意', '贊同': '願意', '認可': '願意',
    '默許': '願意', '准許': '願意', '批准': '願意', '准予': '願意',
    '應承': '願意', '應允': '願意', '贊成': '願意', '同意見': '願意',
    '允准': '願意', '同情': '願意', '贊許': '願意', '贊可': '願意',
    '認同': '願意', '承認': '願意', '承諾': '願意', '應許': '願意',
    '答應': '願意', '應諾': '願意', '樂於': '願意', '甘於': '願意',
    '願為': '願意', '樂為': '願意', '心甘情願': '願意', '甘之如飴': '願意'
}

# 情緒分類
EMOTION_CATEGORIES = {
    '喜悅': 1,    # 正面情緒
    '安心': 1,    # 正面情緒
    '期待': 1,    # 正面情緒
    '正面': 1,    # 正面情緒
    '支持': 1,    # 正面情緒
    '成功': 1,    # 正面情緒
    '謝謝': 1,    # 正面情緒
    '優惠': 1,    # 正面情緒
    '有趣': 1,    # 正面情緒
    '願意': 1,    # 正面情緒
    '驚訝': 0,    # 中性情緒
    '生氣': -1,   # 負面情緒
    '討厭': -1,   # 負面情緒
    '難過': -1,   # 負面情緒
    '焦慮': -1,   # 負面情緒
    '反感': -1,   # 負面情緒
    '失望': -1,   # 負面情緒
    '違規': -1,   # 負面情緒
    '貶低': -1,   # 負面情緒
    '憤怒': -1,   # 負面情緒
    '厭惡': -1,   # 負面情緒
    '質疑': -1,   # 負面情緒
    '諷刺': -1,   # 負面情緒
    '不滿': -1,   # 負面情緒
    '擔憂': -1,   # 負面情緒
    '後悔': -1,   # 負面情緒
    '麻煩': -1,   # 負面情緒
}

"""### ***Data Cleaning & Transformation***"""

# ---- ---- ---- ---- 格式化日期 ---- ---- ---- ---- ---- ---- ---- ----
def format_date(date_string):
    """
    統一處理兩種不同格式的日期:
    1. MM/DD 格式 (來自PTT)
    2. ISO格式 (來自Dcard) 2024-10-19T08:25:47.246Z
    3. 年月日格式 (例如: 2024年10月25日 23:03)
    """
    try:
        if not date_string:
            return None

        if "/" in date_string:  # PTT格式
            date_obj = datetime.strptime(date_string, "%m/%d")
            return f"2024-{date_obj.month:02d}-{date_obj.day:02d}"
        elif "T" in date_string:  # Dcard ISO格式
            date_obj = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            return f"2024-{date_obj.month:02d}-{date_obj.day:02d}"
        elif "年" in date_string and "日" in date_string:  # 新增的年月日格式
            date_obj = datetime.strptime(date_string, "%Y年%m月%d日 %H:%M")
            return f"2024-{date_obj.month:02d}-{date_obj.day:02d}"
        elif "-" in date_string:  # YYYY-MM-DD格式
            date_obj = datetime.strptime(date_string, "%Y-%m-%d")
            return f"2024-{date_obj.month:02d}-{date_obj.day:02d}"
        else:
            return None
    except Exception as e:
        print(f"日期解析錯誤: {e}, 日期字串: {date_string}")
        return None

# ---- ---- ---- ---- 情緒分析 ---- ---- ---- ---- ---- ---- ---- ---- ----
def analyze_emotions(text):
    """
    分析文本中的情緒詞並返回詳細的情緒分析結果
    """
    if not text:
        return defaultdict(int), defaultdict(int)

    words = jieba.lcut(text)
    emotion_word_counts = defaultdict(int)
    emotion_category_counts = defaultdict(int)

    for word in words:
        if word in EMOTION_DICT:
            emotion_word = word
            emotion_category = EMOTION_DICT[word]
            emotion_word_counts[emotion_word] += 1
            emotion_category_counts[emotion_category] += 1

    return emotion_word_counts, emotion_category_counts
# ---- ---- ---- ---- 排名 ---- ---- ---- ---- ---- ---- ---- ---- ----
def get_top_emotions(emotion_dict, category_dict, n=5):
    """
    獲取出現頻率最高的情緒詞和情緒類別
    """
    top_words = sorted(emotion_dict.items(), key=lambda x: x[1], reverse=True)[:n]
    categories = sorted(category_dict.items(), key=lambda x: x[1], reverse=True)

    words = []
    counts = []
    categories_list = []
    category_counts = []

    for word, count in top_words:
        words.append(word)
        counts.append(count)

    while len(words) < n:
        words.append(None)
        counts.append(None)

    for category, count in categories:
        categories_list.append(category)
        category_counts.append(count)

    return words, counts, categories_list, category_counts
# ---- ---- ---- ---- 處理一個集合 ---- ---- ---- ---- ---- ---- ----
def process_single_collection(collection, processed_links):
    """
    處理單個collection中的文檔，依據日期分別處理並避免重複貼文
    """
    batch_size = 5000
    counter = 0
    total_count = collection.count_documents({})

    # 用於儲存每個日期的情緒統計
    date_emotion_counts = defaultdict(lambda: defaultdict(int))
    date_category_counts = defaultdict(lambda: defaultdict(int))

    while counter < total_count:
        cursor = collection.find().skip(counter).limit(batch_size)
        documents = list(cursor)

        if not documents:
            break

        for document in documents:
            try:
                # 取得連結
                link = document["value"].get("連結") or document.get("連結") or document["value"].get("url") or document.get("url")
                if not link:
                    continue

                # 取得發布日期
                publish_date = document["value"].get("發佈日期") or document.get("發布時間") or document["value"].get("url") or document.get("url")
                formatted_date = format_date(publish_date)

                if not formatted_date:
                    continue

                # 取得內容
                content = document["value"].get("內容") or document.get("內容") or document["value"].get("content") or document.get("content")
                if not content:
                    continue

                # 檢查是否已處理過該連結
                if link in processed_links:
                    old_date = processed_links[link]['date']
                    # 如果新的日期較舊，則跳過
                    if formatted_date <= old_date:
                        continue
                    # 如果新的日期較新，需要移除舊的統計
                    old_word_counts = processed_links[link]['word_counts']
                    old_category_counts = processed_links[link]['category_counts']
                    for word, count in old_word_counts.items():
                        date_emotion_counts[old_date][word] -= count
                    for category, count in old_category_counts.items():
                        date_category_counts[old_date][category] -= count

                # 分析新內容的情緒
                word_counts, category_counts = analyze_emotions(content)

                # 更新該日期的情緒統計
                for word, count in word_counts.items():
                    date_emotion_counts[formatted_date][word] += count
                for category, count in category_counts.items():
                    date_category_counts[formatted_date][category] += count

                # 更新處理過的連結資訊
                processed_links[link] = {
                    'date': formatted_date,
                    'word_counts': word_counts,
                    'category_counts': category_counts
                }

            except Exception as e:
                print(f"處理文件時發生錯誤: {str(e)}")
                continue

        counter += batch_size
        print(f"已處理 {counter} 筆文件")

    return date_emotion_counts, date_category_counts
# ---- ---- ---- ---- 處理所有集合 ---- ---- ---- ---- ---- ---- ----
def process_all_collections(collection):
    """
    處理collection並生成結果DataFrame
    """
    processed_links = {}  # 用於追蹤所有文件中的連結
    all_date_emotion_counts = defaultdict(lambda: defaultdict(int))
    all_date_category_counts = defaultdict(lambda: defaultdict(int))

    # 處理collection
    print(f"開始處理 collection: {collection.name}")
    date_emotion_counts, date_category_counts = process_single_collection(
        collection,
        processed_links
    )

    # 合併結果
    for date in date_emotion_counts:
        for word, count in date_emotion_counts[date].items():
            all_date_emotion_counts[date][word] += count
        for category, count in date_category_counts[date].items():
            all_date_category_counts[date][category] += count

    # 建立新的 DataFrame 儲存每日情緒分析結果
    daily_emotions = []

    for date in sorted(all_date_emotion_counts.keys()):
        words, counts, categories, category_counts = get_top_emotions(
            all_date_emotion_counts[date],
            all_date_category_counts[date]
        )

        # 計算主要情緒
        dominant_emotion = categories[0] if categories else None
        emotion_score = sum(count * EMOTION_CATEGORIES[cat]
                          for cat, count in zip(categories, category_counts))

        daily_emotions.append({
            "發佈日期": date,
            "心情詞語1": words[0],
            "次數1": counts[0],
            "心情詞語2": words[1],
            "次數2": counts[1],
            "心情詞語3": words[2],
            "次數3": counts[2],
            "心情詞語4": words[3],
            "次數4": counts[3],
            "心情詞語5": words[4],
            "次數5": counts[4],
            "主要情緒": dominant_emotion,
            "情緒分數": emotion_score
        })

    return pd.DataFrame(daily_emotions)
# ---- ---- ---- ---- 連接至MySQL ---- ---- ---- ---- ---- ---- ----
def connect_to_mysql():
    """
    建立MySQL連接
    """
    connection = None
    try:
        connection = mysql.connector.connect(
            host='34.81.244.193',
            database='PTT',
            user='user3',
            password='password3'
        )

        if connection.is_connected():
            print("成功連線到 MySQL 資料庫")
            cursor = connection.cursor()
            cursor.execute("USE PTT;")
            print("已選擇資料庫：PTT")
            return connection, cursor

    except Error as e:
        print(f"連線錯誤: {e}")

    return connection, None
# ---- ---- ---- ---- 結果匯入MySQL ---- ---- ---- ---- ---- ---- ----
def update_sentiment_table(connection, cursor, result_df):
    """
    更新MySQL資料庫中的情緒分析表
    """
    try:
        # 創建情緒分析表（如果不存在）
        create_table_query = """
        CREATE TABLE IF NOT EXISTS emotion_analysis (
            發佈日期 DATE PRIMARY KEY,
            心情詞語1 VARCHAR(50),
            次數1 INT,
            心情詞語2 VARCHAR(50),
            次數2 INT,
            心情詞語3 VARCHAR(50),
            次數3 INT,
            心情詞語4 VARCHAR(50),
            次數4 INT,
            心情詞語5 VARCHAR(50),
            次數5 INT,
            主要情緒 VARCHAR(50),
            情緒分數 INT
        )
        """
        cursor.execute(create_table_query)

        for index, row in result_df.iterrows():
            # 檢查是否存在相同日期的資料
            check_query = "SELECT * FROM emotion_analysis WHERE 發佈日期 = %s"
            cursor.execute(check_query, (row['發佈日期'],))
            existing_record = cursor.fetchone()

            if existing_record:
                # 如果存在，先刪除該日期的資料
                delete_query = "DELETE FROM emotion_analysis WHERE 發佈日期 = %s"
                cursor.execute(delete_query, (row['發佈日期'],))

            # 插入新的資料
            insert_query = """
                INSERT INTO emotion_analysis (
                    發佈日期, 心情詞語1, 次數1, 心情詞語2, 次數2,
                    心情詞語3, 次數3, 心情詞語4, 次數4, 心情詞語5, 次數5,
                    主要情緒, 情緒分數
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            data = (
                row['發佈日期'],
                row['心情詞語1'],
                row['次數1'],
                row['心情詞語2'],
                row['次數2'],
                row['心情詞語3'],
                row['次數3'],
                row['心情詞語4'],
                row['次數4'],
                row['心情詞語5'],
                row['次數5'],
                row['主要情緒'],
                row['情緒分數']
            )
            cursor.execute(insert_query, data)

        # 提交所有更改
        connection.commit()
        print("資料更新完成")

    except Error as e:
        print(f"資料更新過程發生錯誤: {e}")
        connection.rollback()
        raise e

def main():
    # MongoDB 連線設定
    mongo_uri = "mongodb://user4:password4@35.189.181.117:28017/admin"
    client = MongoClient(mongo_uri)
    db = client['kafka']
    collection = db['Ptt']  # 使用新的統一collection

    # 連線 MySQL
    connection, cursor = connect_to_mysql()

    if connection and cursor:
        try:
            # 處理collection並取得結果
            result_df = process_all_collections(collection)

            # 顯示結果
            print("\n每日情緒分析統計結果：")
            print(result_df)

            # 更新資料庫
            update_sentiment_table(connection, cursor, result_df)

            # 產生簡單的分析報告
            print("\n分析報告：")
            print("-" * 50)

            # 計算整體情緒趨勢
            avg_emotion_score = result_df['情緒分數'].mean()
            print(f"平均情緒分數: {avg_emotion_score:.2f}")

            # 統計最常見的主要情緒
            top_emotions = result_df['主要情緒'].value_counts()
            print("\n最常見的主要情緒:")
            for emotion, count in top_emotions.items():
                print(f"{emotion}: {count}次")

            # 找出情緒波動最大的日期
            result_df['情緒變化'] = result_df['情緒分數'].diff()
            max_change_date = result_df.loc[result_df['情緒變化'].abs().idxmax()]
            print(f"\n情緒波動最大的日期: {max_change_date['發佈日期']}")
            print(f"當日情緒變化幅度: {max_change_date['情緒變化']:.2f}")

        except Exception as e:
            print(f"程式執行過程發生錯誤: {e}")

        finally:
            # 清理連線
            if cursor:
                cursor.close()
            if connection:
                connection.close()
            client.close()
            print("\n資料庫連線已關閉")
    else:
        print("無法建立資料庫連線，程式終止")

"""### ***Main***"""

if __name__ == "__main__":
    main()