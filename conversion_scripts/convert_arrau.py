import sys
import os
import json
import xml.etree.ElementTree as ET
from collections import defaultdict

rst_path = "/data/wgu/ARRAU/data/RST_DTreeBank"
output_path = sys.argv[1]

coref_drop = 0
drop_counter = 0
min_drop = 0
total = 0
relation_count = 0
relation_drop = 0
def get_files(split):
  path = f"{rst_path}/{split}/MMAX"
  files = []
  for f in os.listdir(path):
    if ".header" in f:
      files.append(f.split(".header")[0])
  return files

def get_offsets(span):
  if ".." in span:
    start, end = tuple(span.split(".."))
  else:
    start = span
    end = span
  return (start, end)

def xml_words(word_path):
  tree = ET.parse(word_path)
  root = tree.getroot()
  words = root.iter()
  tokens = []
  token_names = []
  for word in words:
    if "id" in word.attrib:
      word_id = word.attrib["id"]
      word_text = word.text
      tokens.append(word_text)
      token_names.append(word_id)
  token_index_map = dict({name: i for i, name in enumerate(token_names)})
  return (tokens, token_index_map)

def xml_sentences(sentence_path, token_list, token_map):
  tree = ET.parse(sentence_path)
  root = tree.getroot()
  sentence_iter = root.iter()
  sentences = []
  for sentence in sentence_iter:
    if "id" in sentence.attrib:
      span = sentence.attrib["span"]
      start, end = get_offsets(span)
      tokens = token_list[token_map[start]:token_map[end] + 1]
      sentences.append(tokens)
  return sentences

def xml_markables(path, token_list, token_map):
  tree = ET.parse(path)
  root = tree.getroot()
  markable_iter = root.iter()
  markables = {}
  for markable in markable_iter:
    if "id" in markable.attrib:
      markable_id = markable.attrib["id"]
      span = markable.attrib["span"]
      start, end = get_offsets(span)
      markables[markable_id] = (token_map[start], token_map[end])
  return markables


def xml_coref(path, token_list, token_map):
  global drop_counter
  global coref_drop
  global min_drop
  global total
  global relation_count
  global relation_drop

  markable_id_token_map = {}
  relations = []
  tree = ET.parse(path)
  root = tree.getroot()
  coref_iter = root.iter()
  clusters = defaultdict(list)
  for markable in coref_iter:
    if "id" in markable.attrib:
      total += 1
      markable_id = markable.attrib["id"]
      span = markable.attrib["span"]
      if "coref_set" not in markable.attrib:
        coref_drop += 1
        continue
      if ("," in span):
        if "min_ids" not in markable.attrib:
          min_drop += 1
          continue
        else:
          span = markable.attrib["min_ids"]
        drop_counter += 1
      if ".." in span:
        start, end = tuple(span.split(".."))
      else:
        start = span
        end = span
      clusters[markable.attrib["coref_set"]].append([token_map[start], token_map[end]])
      markable_id_token_map[markable_id]=[token_map[start], token_map[end]]
      if "related_rel" in markable.attrib:
        related_rel = markable.attrib["related_rel"]
        if "related_phrase" in markable.attrib:
          related_phrase = markable.attrib["related_phrase"]
          related_phrase = related_phrase.split(";")
          relations.append({"head_phrase": markable_id_token_map[markable_id],"related_phrase": related_phrase, "related_rel": related_rel})
        else:
            relation_drop += 1
      else:
          relation_drop += 1
  coref_clusters = list(clusters.values())
  mapped_relations = []
  for relation in relations:
      related_phrases = relation["related_phrase"]
      temp_related_phrases = []
      for related_phrase in related_phrases:
          if related_phrase in markable_id_token_map.keys():
            temp_related_phrases.append(markable_id_token_map[related_phrase])
      relation["related_phrase"] = temp_related_phrases
      if len(temp_related_phrases) != 0:
        mapped_relations.append(relation)
      else:
        relation_drop += 1
  return coref_clusters,mapped_relations


def get_markables(path, prefix):
  coref_path = f"{path}/markables/{prefix}_coref_level.xml"
  # markable_path = f"{path}/markables/{prefix}_markable_level.xml"
  sentence_path = f"{path}/markables/{prefix}_sentence_level.xml"
  words_path = f"{path}/Basedata/{prefix}_words.xml"
  words = xml_words(words_path)
  sentences = xml_sentences(sentence_path, words[0], words[1])
  # markable = xml_markables(markable_path, words[0], words[1])
  coref,relation = xml_coref(coref_path, words[0], words[1])
  return (sentences, coref, relation)

def process_split(split):
  files = get_files(split)
  path = f"{rst_path}/{split}/MMAX"
  all_files = []
  for prefix in files:
    text, clusters,relations = get_markables(path, prefix)
    new_dictionary = {
      "doc_key": prefix,
      "sentences": text,
      "clusters": clusters,
      "relations": relations
    }
    all_files.append(new_dictionary)
  output = open(f"{output_path}/{split}.jsonlines", 'w+')
  output.write("\n".join([json.dumps(doc) for doc in all_files]))


if __name__ == "__main__":
  process_split("train")
  print(total, drop_counter, coref_drop, min_drop)
  process_split("dev")
  print(total, drop_counter, coref_drop, min_drop)
  process_split("test")
  print(total, drop_counter, coref_drop, min_drop)
