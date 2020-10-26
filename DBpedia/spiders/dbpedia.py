import scrapy
import json
import ipdb
import os
import logging

class DbpediaSpider(scrapy.Spider):
    name = 'dbpedia'
    allowed_domains = ['dbpedia.org']

    def start_requests(self):
        if os.path.exists("res_parse2.json"):
            os.remove("res_parse2.json")
        if os.path.exists("res_parse3.json"):
            os.remove("res_parse3.json")
        if os.path.exists("res_parse4.json"):
            os.remove("res_parse4.json")

        urls = []
        filename = f'./data/DBpedia_entities_in_schema.txt'
        with open(filename, 'r') as f:
            lines = f.readlines()
            for line in lines:
                urls.append(line.strip('\n').split(':')[-1])
        
        count = 0
        for category in urls:
            count += 1
            url = f'http://dbpedia.org/page/Category:' + category
            logging.info('start requesting {}号类别{}'.format(count, category))
            yield scrapy.Request(url=url, meta={'category':category}, callback=self.parse)


    def parse(self, response):
        urls = response.xpath('//a[@rev="dct:subject"]/@href').getall()  #http://dbpedia.org/resource/Igloo
        names = []
        for url in urls:
            names.append(url[28:]) #得到实体名称
        category = response.meta['category']
        count = 0
        for name in names:
            count += 1
            url = "http://dbpedia.org/data/" + name + ".json"
            logging.info('开始请求 类别{} {}号实体{}'.format(category, count, name))
            yield scrapy.Request(url,meta={'Category':category, 'Subject': name, 'Depth': 1},callback=self.parse2)


    def parse2(self,response):
        # 处理实体页
        category = response.meta['Category']
        subject = response.meta['Subject']
        item = dict()
        item['Category'] = category
        item['Subject'] = subject
        dic = response.json()

        item['abstract'] = ''
        if 'http://dbpedia.org/ontology/abstract' in dic['http://dbpedia.org/resource/'+subject]:
            abstract = dic['http://dbpedia.org/resource/'+subject]['http://dbpedia.org/ontology/abstract']
            zh_abstract = ''
            en_abstract = ''
            for key in abstract:
                if key['lang'] == 'zh':
                    zh_abstract = key['value']
                if key['lang'] == 'en':
                    en_abstract = key['value']
            item['abstract'] = {'value':zh_abstract, 'lang':'zh'} if zh_abstract else {'value':en_abstract, 'lang':'en'}

        item['thumbnail'] = ''
        if 'http://dbpedia.org/ontology/thumbnail' in dic['http://dbpedia.org/resource/'+subject]:
            item['thumbnail'] = dic['http://dbpedia.org/resource/'+subject]['http://dbpedia.org/ontology/thumbnail'][0]['value']
        
        item['subjects'] = ''
        if 'http://purl.org/dc/terms/subject' in dic['http://dbpedia.org/resource/'+subject]:
            subjects = dic['http://dbpedia.org/resource/'+subject]['http://purl.org/dc/terms/subject']
            tmp_subjects = list()
            for key in subjects:
                tmp_subjects.append(key['value'][28:]) # http://dbpedia.org/resource/Category:Snow
            item['subjects'] = tmp_subjects
        
        item['label'] = ''
        if 'http://www.w3.org/2000/01/rdf-schema#label' in dic['http://dbpedia.org/resource/'+subject]:
            label = dic['http://dbpedia.org/resource/'+subject]['http://www.w3.org/2000/01/rdf-schema#label']
            zh_label = ''
            en_label = ''
            for key in label:
                if key['lang'] == 'zh':
                    zh_label = key['value']
                if key['lang'] == 'en':
                    en_label = key['value']
            item['label'] = {'value':zh_label, 'lang': 'zh'} if zh_label else {'value':en_label, 'lang': 'en'}

        item['wiki'] = ''
        if 'http://www.w3.org/ns/prov#wasDerivedFrom' in dic['http://dbpedia.org/resource/'+subject]:
            item['wiki'] = dic['http://dbpedia.org/resource/'+subject]['http://www.w3.org/ns/prov#wasDerivedFrom'][0]['value']

        item['wikiPageDisambiguates'] = ''
        disam_url = ''
        if 'http://dbpedia.org/resource/'+subject+'_(disambiguation)' in dic:
            if "http://dbpedia.org/ontology/wikiPageRedirects" not in dic['http://dbpedia.org/resource/'+subject+'_(disambiguation)']:
                disam_url =  'http://dbpedia.org/resource/'+subject+'_(disambiguation)'
        item['wikiPageDisambiguates'] = disam_url

        item['wikiPageRedirects'] = ''
        redirects = []
        for entity in dic.keys():
            if "http://dbpedia.org/ontology/wikiPageRedirects" in dic[entity]:
                redirects.append(entity)
        item['wikiPageRedirects'] =  redirects

        if disam_url != '':
            disam_url = disam_url.replace("resource", "data")
            disam_url += '.json'
            yield scrapy.Request(disam_url, meta={'Category':category, 'Subject': subject}, callback=self.parse3)

        if response.meta['Depth'] == 1:
            filename = f'res_parse2.json'
            json_str = json.dumps(item)
            with open(filename, 'a+') as f:
                f.write(json_str+'\n')
                logging.info('类别{} 实体 {} 已保存'.format(item['Category'], item['label']))
        else:
            filename = f'res_parse4.json'
            json_str = json.dumps(item)
            with open(filename, 'a+') as f:
                f.write(json_str+'\n')
                logging.info('实体{} 歧义实体{} 已保存'.format(item['Category'], item['label']))
    
    def parse3(self,response):
        category = response.meta['Category']
        subject = response.meta['Subject']
        item = dict()
        item['Subject'] = subject
        item['Category'] = category
        dic = response.json()

        wikiPageDisambiguates = dic['http://dbpedia.org/resource/'+subject+'_(disambiguation)']['http://dbpedia.org/ontology/wikiPageDisambiguates']
        tmp_wikiPageDisambiguates = []
        for x in wikiPageDisambiguates:
            tmp_wikiPageDisambiguates.append(x['value'])
    
        item['disambiguates'] = tmp_wikiPageDisambiguates
        filename = f'res_parse3.json'
        json_str = json.dumps(item)
        with open(filename, 'a+') as f:
            f.write(json_str+'\n')
            logging.info('实体{} 的歧义实体列表已保存'.format(subject))

        count = 0
        for url in tmp_wikiPageDisambiguates:
            count += 1
            d_subject = url[28:]
            url = url.replace("resource", "data")
            url += '.json'
            logging.info('开始请求 实体{} {}号歧义实体{}'.format(category, count, d_subject))
            yield scrapy.Request(url, meta={'Category':subject, 'Subject': d_subject, 'Depth':2}, callback=self.parse2)