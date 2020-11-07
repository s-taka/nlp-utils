import pprint as pp
import re
import os, sys
import time

import tensorflow_hub as hub
import numpy as np
import tensorflow_text
import argparse

embed = hub.load("https://tfhub.dev/google/universal-sentence-encoder-multilingual/3")

# from transformers import MarianMTModel, MarianTokenizer
# tokenizer_ja_en = MarianTokenizer.from_pretrained('Helsinki-NLP/opus-mt-ja-en')
# model_ja_en = MarianMTModel.from_pretrained('Helsinki-NLP/opus-mt-ja-en')

import MeCab
wakati = MeCab.Tagger("-Owakati")

from bs4 import BeautifulSoup
from nltk.tokenize import sent_tokenize
import nltk
nltk.download('punkt')


def cos_sim(lhs, rhs):
    return np.dot(lhs, rhs) / (np.linalg.norm(lhs) * np.linalg.norm(rhs))


def get_html(txt):
    ret = txt[txt.find("<html"):txt.find("</html")] + "</html>"
    return ret.replace("<CR>", "\r").replace("<LF>", "\n").replace("<TAB>", " ")


def get_article(html, url, bp, mode = 'ja'):
    sp = BeautifulSoup(html, 'html.parser')
    ret = "".join([n.get_text() for n in sp.select(bp)])
    if len(ret) > 100:
        return ret
    return ""


def clean_txt(txt):
    ret = re.sub(r"^\r?\n?", "", txt.replace(u'\xa0', u' '))
    ret = re.sub(r"(\r\n)+", "\n", ret)
    ret = re.sub(r"\n+", "\n", ret)
    ret = re.sub(r"\r+", "\n", ret)
    ret = re.sub(r"^[\.\,]?[\s\r\t\n]*", "", ret)
    ret = re.sub(r"[\s\r\t\n]+$", "", ret)
    return ret


def to_one_line(txt):
    return re.sub("^(\s|\W)+", "", txt.replace("\r", "").replace("\n", "").replace("\t", ""))


def sent_tokenize_ja(txt):
    jp_sent_tokenizer = nltk.RegexpTokenizer(u'[^　「」！？。]*[！？。]')
    ary = jp_sent_tokenizer.tokenize(txt)
    return [v for v in filter(lambda x: len(x) > 3, [clean_txt(s) for s in ary])]

# If you process news articles, it's better than the above method.
def sent_tokenize_ja_news(txt):
    ary = re.split("(。|！|？|\n|\s\s)",txt)
    for i in range(len(ary) - 1):
        if re.search("(「|「)", ary[i]) and not re.search("(」|」)", ary[i]) and re.search("(」|」)", ary[i+1]) :
            ary[i] = ary[i] + ary[i+1]
            i+=1
    return ["{}。".format(v) for v in filter(lambda x: len(x) > 3, [re.sub("^[\s\r\n]+","",clean_txt(s)) for s in ary])]

def sent_tokenize_en(txt):
    ary = sent_tokenize(clean_txt(re.sub("\.", ". ", txt)))
    return [v for v in filter(lambda x: len(x) > 3, [clean_txt(s) for s in ary])]


def get_align(ja_sents, en_sents, threshold=0.4, decay=0.05):
    ja_vec = embed(ja_sents)
    en_vec = embed(en_sents)
    offsets = []
    for jidx, jv in enumerate(ja_vec):
        max_sim, ofs = max([(cos_sim(jv, ev), eidx - jidx) for eidx, ev in enumerate(en_vec)])
        if max_sim > threshold and abs(ofs) < 10:
            offsets.append(ofs)
    if len(offsets):
        off_set = sum(offsets) / len(offsets)
    else:
        off_set = 0
    ret = []
    jidx = 0
    while jidx < len(ja_vec):
        jv = ja_vec[jidx]
        max_sim, eidx = max(
            [((1.0 - min(abs(eidx - (jidx + off_set)) * decay, 1.0)) * cos_sim(jv, ev), eidx) for eidx, ev in
             enumerate(en_vec)])
        add_below_sim_ja = 0.0
        add_below_sim_en = 0.0
        if max_sim < threshold:
            jidx += 1
            continue
        if jidx + 1 < len(ja_vec):
            add_below_sim_ja = (1.0 - min(abs(eidx - (jidx + off_set)) * decay, 1.0)) * cos_sim(
                embed([ja_sents[jidx] + ja_sents[jidx + 1]])[0], en_vec[eidx])
        if eidx + 1 < len(en_vec):
            add_below_sim_en = (1.0 - min(abs(eidx - (jidx + off_set)) * decay, 1.0)) * cos_sim(ja_vec[jidx], embed(
                [en_sents[eidx] + en_sents[eidx + 1]])[0])
        if add_below_sim_ja > max_sim + decay * 2 and add_below_sim_ja > threshold + decay:
            ret.append((add_below_sim_ja, ja_sents[jidx] + ja_sents[jidx + 1], en_sents[eidx]))
            jidx += 1
            off_set -= 1.0
        elif add_below_sim_en > max_sim + decay * 2 and add_below_sim_en > threshold + decay:
            ret.append((add_below_sim_en, ja_sents[jidx], en_sents[eidx] + en_sents[eidx + 1]))
            off_set += 1.0
        elif max_sim > threshold:
            ret.append((max_sim, ja_sents[jidx], en_sents[eidx]))
        jidx += 1
    return ret


def get_title(html, url, mode):
    sp = BeautifulSoup(html, 'html.parser')
    return "".join([n.get_text() for n in sp.select('title')])


def get_description(html, url, mode):
    sp = BeautifulSoup(html, 'html.parser')
    return sp.find('meta', attrs={'property': 'og:description'})["content"]


def count_words(sentence):
    return len(nltk.word_tokenize(sentence))


def filter_aligns(align_sents, threshold=0.7, mode="mecab"):
    # sort and uniq_en
    en_sents = {}
    ret = []
    if mode != "translate":
        threshold = threshold * 0.9
    for tpl in sorted(align_sents, reverse=True):
        en_s = tpl[2]
        if en_s in en_sents:
            continue
        else:
            en_sents[en_s] = 1
            ret.append(tpl)
    # check word count
    txts = [v[1] for v in ret]
    # batch = tokenizer_ja_en.prepare_translation_batch(src_texts=txts)
    # gen = model_ja_en.generate(**batch)
    # txt_en = tokenizer_ja_en.batch_decode(gen, skip_special_tokens=True)
    if mode == "translate":
        # translated = model_ja_en.generate(**tokenizer_ja_en.prepare_translation_batch(txts))
        # txt_en = [tokenizer_ja_en.decode(t, skip_special_tokens=True) for t in translated]
        pass
    else:
        txt_en = [wakati.parse(s).split() for s in txts]
    #print(txt_en)
    filtered_ret = []
    for idx, tpl in enumerate(ret):
        if mode == "translate":
            ja_en_words = 1.0 + count_words(txt_en[idx])
        else:
            ja_en_words = 1.0 + len(txt_en[idx])
        original_en_words = 1.0 + count_words(tpl[2])
        if threshold < ja_en_words / original_en_words and ja_en_words / original_en_words < 1.0 / threshold:
            filtered_ret.append(tpl)

    return filtered_ret


def filter_sents(sents):
    for_filter_sents = ['Share this with', 'Email', 'Twitter', 'Facebook','Pinterest', 'WhatsApp',
                    'Messenger','Hatena', 'Mixi', 'Line', 'このリンクをコピーする', 'LinkedIn']
    def _filter_match(lhs, rhs):
        if (rhs.find(lhs) != -1 or lhs.find(rhs) != -1) and abs(len(lhs) - len(rhs)) < 5:
            return True
        return False
    ret = []
    for s in sents:
        if max([_filter_match(s, fs) for fs in for_filter_sents]):
            continue
        else:
            ret.append(s)
    return ret


def get_align_html(ja_html, en_html, url = "", ja_bp="p", en_bp="p"):
    align_texts = []

    # ja_title = get_title(ja_html, url, "ja")
    # en_title = get_title(en_html, url, "en")
    #
    # sim_title = cos_sim(embed([ja_title])[0], embed([en_title])[0])
    # if sim_title > 0.5:
    #     align_texts.append((sim_title, ja_title, en_title))

    # ja_description = get_description(ja_html, url, "ja")
    # en_description = get_description(en_html, url, "en")
    # sim_description = cos_sim(embed([ja_description])[0], embed([en_description])[0])
    # if sim_description > 0.5:
    #     align_texts.append((sim_description, ja_description, en_description))

    ja_article = get_article(ja_html, url, ja_bp)
    ja_article = re.sub("(\r\n|\r|\n|\t){2,}", ". ", ja_article)
    en_article = get_article(en_html, url, en_bp)
    en_article = re.sub("(\r\n|\r|\n|\t){2,}", ". ", en_article)
    ja_sents = filter_sents(sent_tokenize_ja(clean_txt(ja_article)))
    en_sents = filter_sents(sent_tokenize_en(clean_txt(en_article)))

    if len(ja_sents) < 50:
        threshold = 0.3
    elif len(ja_sents) < 100:
        threshold = 0.4
    else:
        threshold = 0.5

    if re.search("抄訳", ja_article):
        threshold = 0.6

    if len(ja_sents) * 2 < len(en_sents):
        threshold = 0.6

    align_sents = get_align(ja_sents, en_sents, threshold=threshold)
    for tpl in align_sents:
        print("align_candidate=\t{}\t{}\t{}\n".format(to_one_line(tpl[1]), to_one_line(tpl[2]), tpl[0]))


    align_sents_filter = filter_aligns(align_sents)
    print("original={} filter={} \t url={}".format(len(align_sents), len(align_sents_filter), url))

    align_texts += align_sents_filter

    return align_texts


def main():
    parser = argparse.ArgumentParser(description='align en_sentence and ja_sentence')
    parser.add_argument('ja_html', help='html file (lang=ja)')
    parser.add_argument('en_html', help='html file (lang=en)')
    parser.add_argument('--ja_bp', help='BeautifulSoup selector for ja_html')
    parser.add_argument('--en_bp', help='BeautifulSoup selector for en_html')

    args = parser.parse_args()

    ja_html_file = args.ja_html
    en_html_file = args.en_html
    ja_bp = "p"
    if args.ja_bp:
        ja_bp = args.ja_bp
    en_bp = "p"
    if args.en_bp:
        en_bp = args.en_bp

    ja_html = open(ja_html_file).read()
    en_html = open(en_html_file).read()

    aligns = get_align_html(ja_html, en_html, ja_bp=ja_bp, en_bp=en_bp)
    for tpl in aligns:
        print("{}\t{}\t{}\n".format(to_one_line(tpl[1]), to_one_line(tpl[2]), tpl[0]))
    return


if __name__ == "__main__":
    main()
