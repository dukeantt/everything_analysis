"""
This is a script to correct sentences heuristically, using abbreviation_json and
telex_typing_json file.
"""

import argparse
import json
import textdistance
import re
import unidecode
import operator
import string
import pickle
from collections import deque

import os
script_dir = os.path.dirname(__file__)

vnmese_alphabet_dict = json.load(open(script_dir + "/data/vnmese_alphabet_json.json", encoding='utf-8-sig'))

hard_code_word_map_og = json.load(open(script_dir + "/data/hard_code_word_map_json.json", encoding='utf-8-sig'))
hard_code_word_map = {}
for k, v in hard_code_word_map_og.items():
    if isinstance(v, list):
        for i in v:
            hard_code_word_map.update({i: k})
    else:
        hard_code_word_map.update({v: k})

with open(script_dir + '/data/begin.pkl', 'rb') as word:
    begin_prob = pickle.load(word)
begin_prob = dict(begin_prob)

with open(script_dir + '/data/word_prob.pkl', 'rb') as word:
    word_data = pickle.load(word)


def remove_dau_cau(input_string):
    vnmese_alphabet_dict_better = {
        'a': ['a', 'à', 'ả', 'ã', 'á', 'ạ'],
        'ă': ['ằ', 'ẳ', 'ẵ', 'ắ', 'ặ'],
        'â': ['ầ', 'ẩ', 'ẫ', 'ấ', 'ậ'],
        'e': ['e', 'è', 'ẻ', 'ẽ', 'é', 'ẹ'],
        'ê': ['ề', 'ể', 'ễ', 'ế', 'ệ'],
        'i': ['i', 'ì', 'ỉ', 'ĩ', 'í', 'ị'],
        'o': ['o', 'ò', 'ỏ', 'õ', 'ó', 'ọ'],
        'ô': ['ồ', 'ổ', 'ỗ', 'ố', 'ộ'],
        'ơ': ['ờ', 'ở', 'ỡ', 'ớ', 'ợ'],
        'u': ['u', 'ù', 'ủ', 'ũ', 'ú', 'ụ'],
        'ư': ['ừ', 'ử', 'ữ', 'ứ', 'ự'],
        'y': ['y', 'ỳ', 'ỷ', 'ỹ', 'ý', 'ỵ'],
    }
    input_string_chars = split_sentence_to_char(input_string)
    for char_index, input_string_char in enumerate(input_string_chars):
        for char, variations in vnmese_alphabet_dict_better.items():
            if input_string_char in variations:
                input_string_chars[char_index] = char
    output_string = ''.join(input_string_chars)
    return output_string


def remove_duplicate_char(input_string):
    """Function remove duplicate char"""
    input_string_chars = split_sentence_to_char(input_string)
    # remove duplicated phrase (2 chars) next to each other
    index_to_remove = []
    for char_index, char in enumerate(input_string_chars):
        if len(index_to_remove) > 0:
            if (char_index + 2) - index_to_remove[0][0] == 1:
                continue
        if char_index + 3 < len(input_string_chars):
            char_double = input_string_chars[char_index:char_index + 2]
            char_double_following = input_string_chars[char_index + 2:char_index + 4]
            if char_double == char_double_following:
                index_to_remove.insert(0, (char_index + 2, char_index + 4))

    for value in index_to_remove:
        del input_string_chars[value[0]:value[1]]

    # remove duplicated chars next to each other
    index_to_remove = []
    for char_index, char in enumerate(input_string_chars):
        if char in ["u","m","x","e"]:
            continue
        if char_index + 1 < len(input_string_chars):
            char_following_1 = input_string_chars[char_index + 1]
            if char == char_following_1:
                index_to_remove.insert(0, char_index + 1)
    for value in index_to_remove:
        del input_string_chars[value]

    output_string = ''.join(input_string_chars)
    return output_string


def split_word_n_number(input_string):
    """Function Split word and number literally"""
    # number after word
    head = input_string.rstrip('0123456789')
    tail = input_string[len(head):]
    if head == '' or tail == '':
        # number before word
        tail = input_string.lstrip('0123456789')
        head = input_string.replace(tail, '')
    return head, tail


def compare(word1, word2):
    """Function Compare 2 word """
    i = 0
    if len(word2) == 0:
        return 0
    if 0.6 <= (len(word1) / len(word2)) <= 1.65:
        for char1, char2 in zip(word1, word2):
            if char1 == char2:
                i += 1
        if len(word1) < len(word2):
            if word1[-1] == word2[-1]:
                i += 1
        if len(word2) > len(word1):
            return i / len(word2) * 100
        else:
            return i / len(word1) * 100
    return 0


def create_replace_word_list(i, output_text_list,current_word, ignore, replace_word_list):
    """Function create replace word list"""
    # Check i+1 index to avoid key error
    word = current_word
    next_word_has_diacritic = False
    if i + 1 < len(output_text_list):
        next_word = output_text_list[i + 1]
        next_word_og = next_word
        next_word = unidecode.unidecode(next_word)
        if next_word_og != next_word and next_word_og != remove_dau_cau(next_word_og):
            next_word_has_diacritic = True
        try:
            if not bool(re.search(r'\d', word)):
                word_clean = re.sub('\,|\.', '', word)
            else:
                word_clean = word

            next_word_prob = word_data[word_clean]
            # Sort the word from lowest prob to highest
            #              so that when add to Replace Word List the word that have high prob can be at top position
            next_word_prob.sort(key=operator.itemgetter(1), reverse=False)

        # If word does not have next word prob -> skip and leave it's next word be
        except KeyError:
            return 0

        # Remove duplicate stand next to each other in word (eg: giuongngng)
        if next_word not in ignore and next_word_og not in ignore:
            next_word = remove_duplicate_char(next_word)
            next_word_og = remove_duplicate_char(next_word_og)

        # Loop word prob to fill the Replace Word List
        for index, value in enumerate(next_word_prob):
            value_x = value[0]
            value_x = unidecode.unidecode(value_x)
            next_word = next_word.translate(str.maketrans('', '', string.punctuation))
            next_word_og = next_word_og.translate(str.maketrans('', '', string.punctuation))

            # If word match 100% with word prob -> add to first index in list, don't care about prob
            if value[0].lower() == next_word_og.lower():
                if replace_word_list[0][0] == "first_default":
                    replace_word_list[0] = value
                else:
                    replace_word_list.insert(0, value)

            # If unidecode word match -> add to second index in list
            elif value_x == next_word and not next_word_has_diacritic:
                replace_word_list.insert(1, value)

            # If unidecode word match 66.5% and len > 4 -> add  to last index in list but for the word that have len < 4
            # elif compare(next_word, value_x) > 66.5 and compare(next_word, value_x) <= 100 and len(next_word) < len(value_x):
            # replace_word_list.insert(-1, value)

            # If unidecode word match 63% and len > 4 -> add  to last index in list
            elif compare(next_word_og, value[0]) >= 61 and (len(next_word) >= 4 or len(value_x) >= 4):
                replace_word_list.insert(-1, value)
    return replace_word_list


def create_word_pair_list(output_text_list, word_pair_index, ignore):
    """Function Create word pair list"""
    for index, value in enumerate(output_text_list):
        try:
            if value in ignore:
                continue
            if value.isalpha():
                continue
            if not (represents_int(value[0]) and represents_int(value[-1])):
                value1, value2 = split_word_n_number(value)
                if value1 == '' or value2 == '':
                    continue
                output_text_list[index] = value1
                output_text_list.insert(index + 1, value2)
                word_pair_index.append((index, index + 1))
        except IndexError:
            print(value)
    return word_pair_index, output_text_list


def fix_first_word(output_text, output_text_list, step1_fixed):
    """Function fix first word"""
    temp_first_word = output_text_list[0]
    if not temp_first_word.isalpha():
        return output_text_list

    first_word_list = []
    found_match_word = False
    should_keep_og_first_word = False
    first_word_has_diacritic = False
    # If word in step1_fixed then return
    for item in step1_fixed:
        start_index = [match.start() for match in re.finditer(item, output_text)]
        if 0 in start_index:
            return output_text_list
    # ----------------

    # Should we keep original first word
    try:
        if not bool(re.search(r'\d', temp_first_word)):
            temp_first_word_clean = re.sub('\,|\.', '', temp_first_word)
        else:
            temp_first_word_clean = temp_first_word
        word_prob_of_first_word = word_data[temp_first_word_clean]

        second_word = output_text_list[1]
        if second_word in [i[0] for i in word_prob_of_first_word]:
            should_keep_og_first_word = True
    except KeyError:
        should_keep_og_first_word = False
    if temp_first_word != unidecode.unidecode(temp_first_word):
        first_word_has_diacritic = True
    if first_word_has_diacritic and should_keep_og_first_word:
        return output_text_list
    #-----------------

    for first_wordx, prob in begin_prob.items():
        if compare(temp_first_word.lower(), first_wordx.lower()) > 90:
            first_word_list.insert(0,(first_wordx,prob))
            found_match_word = True
        elif compare(temp_first_word.lower(), first_wordx.lower()) >= 60 or unidecode.unidecode(temp_first_word.lower()) == unidecode.unidecode(first_wordx.lower()):
            first_word_list.append((first_wordx, prob))
    if len(first_word_list) > 0:
        point_dict = {}
        first_word_list_og = first_word_list.copy()
        first_word_list.sort(key=operator.itemgetter(1), reverse=True)
        second_word = output_text_list[1]
        for first_wordx_index, first_wordx in enumerate(first_word_list):
            try:
                if not bool(re.search(r'\d', temp_first_word)):
                    first_word_x_clean = re.sub('\,|\.', '', first_wordx[0])
                else:
                    first_word_x_clean = first_wordx[0]
                next_word_prob = word_data[first_word_x_clean]
            except KeyError:
                continue
            for next_word_index, next_wordx in enumerate(next_word_prob):
                if compare(second_word.lower(), next_wordx[0].lower()) > 65 or unidecode.unidecode(second_word.lower()) == unidecode.unidecode(next_wordx[0].lower()):
                    point_dict[first_wordx[0] + " " + next_wordx[0]] = first_wordx[1] + next_wordx[1]
        if len(point_dict) > 0:
            if found_match_word and len(point_dict) > 1:
                for item in point_dict.items():
                    if item[0] == output_text_list[0] + ' ' + output_text_list[1]:
                        output_text_list[0] = item[0].split()[0]
                        return output_text_list
                sub_list_point_dict = list(point_dict.items())[1:]
                sub_list_point_dict.sort(key=operator.itemgetter(1),reverse=True)
                if sub_list_point_dict[0][1] / point_dict[list(point_dict.keys())[0]] < 1.63:
                    output_text_list[0] = list(point_dict.keys())[0].split()[0]
                    return output_text_list
            # Add 1 point to move all match unicode to top position
            for phrase, point in point_dict.items():
                two_first_word = output_text_list[0]+' '+output_text_list[1]
                two_first_word = unidecode.unidecode(two_first_word)
                if two_first_word == unidecode.unidecode(phrase):
                    point_dict[phrase] = 1 + point

            point_dict = {k: v for k, v in reversed(sorted(point_dict.items(), key=lambda item: item[1]))}
            real_first_word = list(point_dict.keys())[0].split()[0]
            output_text_list[0] = real_first_word
        elif should_keep_og_first_word:
            return output_text_list
        elif len(first_word_list) > 0 and found_match_word:
            output_text_list[0] = first_word_list_og[0][0]
        elif len(first_word_list) > 0 and not found_match_word:
            output_text_list[0] = first_word_list[0][0]
        else:
            output_text_list[0] = temp_first_word
    else:
        output_text_list[0] = temp_first_word
    return output_text_list


def handle_special_case(output_text_list):
    for index, value in enumerate(output_text_list):
        if not value.isalpha() and value != " " and len(value) > 5:
            list_of_float_number = re.findall("\d+\,\d+cm|\d+\.\d+cm|\d+\,\d+m|\d+\.\d+m|\d+\,\d+|\d+\.\d+", value)
            if len(list_of_float_number) >= 2:
                replace_string = ''
                for index2, value2 in enumerate(list_of_float_number):
                    if value2 != '':
                        if index2 == len(list_of_float_number) - 1:
                            replace_string += value2
                        else:
                            replace_string += value2 + ' x '
                output_text_list[index] = replace_string
    return output_text_list


def split_sentence_to_char(word):
    """Function Split sentence to char literally"""
    return [char for char in word]


def represents_int(input_string):
    """Function Check if the string represents int"""
    try:
        int(input_string)
        return True
    except ValueError:
        return False


def is_brand(word: str, ignore_keywords: list, score=0.70):
    """
    Function to check brand.
    Using Ratcliff-Obershelp similarity
    Inp:
     score: Ratcliff-Obershelp score lager than score
    Return: True mean that it is brand
    """
    for ig in ignore_keywords:
        ig = ig.lower()
        if compare(word, ig) > score*100 and textdistance.ratcliff_obershelp(ig, word) > score:
            return True
    return False


def dict_generate(abbreviation, telex):
    """
    Function to create dictionary from json file
    (default locations:"./data/abbreviation_json.json" and "./data/telex_typing_json.json")
    Since these 2 json file are used to create error (correct word becomes wrong word), we have to reverse them
    Also create a list of keyword to ignore (brand names,...)
    """
    abb_inv = json.load(open(abbreviation, encoding='utf-8-sig'))
    telex_inv = json.load(open(telex, encoding='utf-8-sig'))

    abb_dict = {}
    telex_dict = {}

    for k, v in abb_inv.items():
        if isinstance(v, list):
            for i in v:
                abb_dict.update({i: k})
        else:
            abb_dict.update({v: k})

    for k, v in telex_inv.items():
        if isinstance(v, list):
            for i in v:
                telex_dict.update({i: k})
        else:
            telex_dict.update({v: k})

    with open(script_dir + "/data/ignore_words.txt", "r", encoding='utf-8-sig') as ignore:
        ignore_keywords = [line.strip("\n").lower() for line in ignore.readlines()]

    return abb_dict, telex_dict, ignore_keywords


def num_abb_correction(input_string):
    """
    Fixes common abbreviation with numeric value
    Ex: 6.5th -> 6.5 tháng
    """
    if re.search(r'\dth$', input_string):
        string = input_string[:-2] + ' tháng'
        return string, True
    if re.search(r'\dtr$', input_string):
        string = input_string[:-2] + ' triệu'
        return string, True
    return input_string, False


def telex_correction(input_string, block_length, telex_dict):
    """
    Correct typing error by scanning a block of characters
    from the input string (usually 2 or 3)
    then replace them with the correct typed word
    """
    correct_string = ""
    i = 0
    while i < len(input_string):
        chunk = input_string[i: (i + block_length)]
        if chunk.lower() in telex_dict.keys() and (i + block_length <= len(input_string)):
            chunk = telex_dict[chunk.lower()]
            i = i + block_length  # -> giuowng -> giươg
            # i += len(chunk)
            correct_string = correct_string + chunk
        elif i < len(input_string):
            correct_string = correct_string + input_string[i]
        i = i + 1

    return correct_string


def correction(input_sentence, abb_dict, telex_dict, ignore):
    # Todo Correction step 1
    """
    Fix a sentence by correcting abbreviation errors first, typing second
    In case a word has multiple correction solution (ex: 'đc' can either means 'được' or 'địa chỉ')
    we use the one listed after in the json file (in this case 'được')
    We recommend putting words with higher occurences after words with lower occurrences in json file
    because of this mechanism
    """
    correct_sentence = ""
    # pre-clean step to remove most symbols
    sentence = re.sub('\!|\@|\#|\$|\^|\&|\(|\)|\<|\>|\?|\"|\'', '', input_sentence)
    sentence = sentence.split()
    # list to append words that had been fixed from step1
    step1_fixed = []

    for word in sentence:
        if not bool(re.search(r'\d', word)):
            word = re.sub('\,|\.', '', word)
        word, check_num_abb = num_abb_correction(word)
        if check_num_abb:
            correct_sentence = correct_sentence + word + " "
            step1_fixed.append(word)
            continue

        if is_brand(word.lower(), ignore):
            correct_sentence = correct_sentence + word + " "
            step1_fixed.append(word)
            continue

        if word.lower() in abb_dict.keys():
            word = abb_dict[word.lower()]
            step1_fixed.append(word)
        else:
            # current word
            old_word = word
            word = telex_correction(word, 3, telex_dict)
            word = telex_correction(word, 2, telex_dict)
            # if `word` if fixed then append it to the list
            if old_word != word:
                step1_fixed.append(word)
        correct_sentence = correct_sentence + word + " "

    correct_sentence = correct_sentence.strip()

    return correct_sentence, step1_fixed


def correction_with_dict(input_string, abb_dict, telex_dict, ignore, step1_fixed):
    # Todo Correction step 2
    """Preprocess input string and pass it to function char_dict and word_dict"""
    input_text = input_string.strip()
    input_text = input_text.lower()
    input_text1 = input_text
    step1_fixed = [x.lower() for x in step1_fixed]
    output_text = correct_sentence_with_word_dict(input_text1, abb_dict, ignore, step1_fixed)
    return output_text


def correct_sentence_with_word_dict(output_text, abb_dict, ignore, step1_fixed):
    """Correct input string with character dictionary"""
    # Todo Correction with word_dict
    output_text_list = output_text.split()
    # --------------------- Index of ignore word in step1_fixed ------------

    # -------------------------- Fix first word --------------------------
    # Todo fix first word

    if output_text_list[0] not in ignore and len(output_text_list) > 1:
        output_text_list = fix_first_word(output_text, output_text_list, step1_fixed)

    # --------------------------Handle 1m3x1m4 1,3mx1,4m 1,3cmx1,3m----------------
    # Todo fix 1m3x1,3m
    output_text_list = handle_special_case(output_text_list)

    # -------------------------- Get word-pair --------------------------
    # Todo Create word pair list
    word_pair_index = []
    word_pair_index, output_text_list = create_word_pair_list(output_text_list, word_pair_index, ignore)

    # -------------------------- Main part --------------------------
    previous_replace_word_list = []
    step_to_skip = 0

    this_word_is_in_step1_fixed = False
    for i in range(len(output_text_list)):

        # skip the words in step1_fixed
        if step_to_skip != 0:
            step_to_skip -= 1
            continue
        # -------------------------------

        replace_word_list = [('first_default', 0), ('last_default', 0)]
        word = output_text_list[i]
        next_word = ''
        next_word_og = ''

        if i + 1 < len(output_text_list):
            next_word = output_text_list[i + 1]
            next_word_og = next_word
            next_word = unidecode.unidecode(next_word)

        # check if next_word in step1_fixed
            is_word_in_step1_fixed = False
            if next_word_og.isalpha():
                for item in step1_fixed:
                    if next_word_og in item:
                        len_of_ignore_word = len(item.split())
                        if i+1+len_of_ignore_word > len(output_text_list):
                            continue
                        else:
                            phrase = output_text_list[i+1:i+1+len_of_ignore_word]
                            phrase = ' '.join(phrase)
                            if phrase == item:
                                is_word_in_step1_fixed = True
                                this_word_is_in_step1_fixed = True
                                step_to_skip = len(item.split()) - 1
                                break

            if is_word_in_step1_fixed:
                continue
            # ------------------------------------------

        # Todo Create replace word list
        replace_word_list = create_replace_word_list(i, output_text_list,word, ignore, replace_word_list)
        if replace_word_list == 0:
            continue

        # ----------------------------------- IMPORTANT PART HERE ----------------------------------

        temp_replace_word_list = [x for x in replace_word_list if (x[0] != 'first_default' and x[0] != 'last_default')]
        if this_word_is_in_step1_fixed == False:
            if len(temp_replace_word_list) == 0 and not represents_int(word):
                for index ,value in enumerate(previous_replace_word_list):
                    if value[0] == "first_default" or value[0] == "last_default":
                        continue
                    new_current_word = value[0]
                    replace_word_list2 = [('first_default', 0), ('last_default', 0)]
                    new_replace_word_list = create_replace_word_list(i,output_text_list,new_current_word,ignore,replace_word_list2)
                    temp_replace_word_list = [x for x in new_replace_word_list if (x[0] != 'first_default' and x[0] != 'last_default')]
                    if len(temp_replace_word_list) > 0:
                        if output_text_list[i] not in ignore:
                            if len(output_text_list[i]) > 1:
                                output_text_list[i] = new_current_word
                            replace_word_list = new_replace_word_list
                        break
        else:
            this_word_is_in_step1_fixed = False

        #  3 groups in Replace Word List:
        # First  priority is the word at first index: word match 100% with word prob
        # Second priority is the words from second index: unidecode match with word prob
        # Third  priority is the rest: unidecode match 63%

        # Todo replace word with replace word list
        # If there is first priority word go here
        # But sometimes word at first index is not as good as at second index so compare their prob points
        if replace_word_list[0][0] != 'first_default':
            replace_word_sub_list = replace_word_list[1:]
            replace_word_sub_list.sort(key=operator.itemgetter(1), reverse=True)
            if replace_word_sub_list[0][1]/replace_word_list[0][1] > 10:
                if i + 2 < len(output_text_list):
                    next_word_of_next_word = output_text_list[i + 2]
                    if not bool(re.search(r'\d', replace_word_list[0][0])):
                        replace_word_list_first_item_clean = re.sub('\,|\.', '', replace_word_list[0][0])
                    else:
                        replace_word_list_first_item_clean = replace_word_list[0][0]
                    word_data_of_next_word = word_data[replace_word_list_first_item_clean]
                    if next_word_of_next_word in word_data_of_next_word:
                        output_text_list[i + 1] = replace_word_list[0][0]
                    else:
                        output_text_list[i + 1] = replace_word_sub_list[0][0]
                elif not represents_int(output_text_list[i + 1]) and output_text_list[i + 1].isalpha():
                    output_text_list[i + 1] = replace_word_sub_list[0][0]
            else:
                if not represents_int(output_text_list[i + 1]) and output_text_list[i + 1].isalpha():
                    output_text_list[i + 1] = replace_word_list[0][0]
            replace_word_list.sort(key=operator.itemgetter(1), reverse=True)
         # If there is no first priority but there are second priority and third priority
        elif replace_word_list[1][0] != 'last_default':
            for replace_word_index, replace_word in enumerate(replace_word_list):
                if next_word != '' and next_word.isalpha():
                        # The word that match unidecode is better  maybe
                    if unidecode.unidecode(replace_word[0]) == unidecode.unidecode(next_word):
                        # Add 1 point so all unidecode word match can go to top after sort
                        new_point = replace_word[1] + 1
                        replace_word_list[replace_word_index] = (replace_word[0], new_point)
            replace_word_list.sort(key=operator.itemgetter(1), reverse=True)
            if not represents_int(output_text_list[i + 1]) and output_text_list[i + 1].isalpha():
                output_text_list[i + 1] = replace_word_list[0][0]

        previous_replace_word_list = replace_word_list

    # Handle word pair (eg: 2m, 1tr, 2 tuan, 4-600k, etc)
    for value in word_pair_index:
        word1_index = value[0]
        word2_index = value[1]
        word1 = output_text_list[word1_index]
        word2 = output_text_list[word2_index]
        if (len(word2) <= 2 and word2.isalpha()) or (len(word1) <= 2 and word1.isalpha()) or (
                not word1.isalpha() and not word2.isalpha()):
            output_text_list[word1_index] = word1 + word2
            output_text_list[word2_index] = ''

    # Final part
    output_text_list = [word for word in output_text_list if word != '']

    # Fix meaningless words
    for index, value in enumerate(output_text_list):
        if value in list(hard_code_word_map.keys()):
            output_text_list[index] = hard_code_word_map[value]
    output_text = ' '.join(output_text_list)
    return output_text


def do_correction(input_sentence):
    # Todo Main
    """main"""
    abbreviation_file = script_dir + "/data/abbreviation_json.json"
    telex_file = script_dir + "/data/telex_typing_json.json"
    abb_dict, telex_dict, ignore = dict_generate(abbreviation_file, telex_file)

    correct_sentence, step1_fixed = correction(input_sentence, abb_dict, telex_dict, ignore)
    if correct_sentence != "":
        correct_sentence = correction_with_dict(correct_sentence, abb_dict, telex_dict, ignore, step1_fixed)
    return correct_sentence



