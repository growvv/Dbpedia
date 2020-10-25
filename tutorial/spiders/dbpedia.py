import scrapy
import json
import ipdb
import os

class DbpediaSpider(scrapy.Spider):
    name = 'dbpedia'
    allowed_domains = ['dbpedia.org']
    start_urls = ['http://http://dbpedia.org/page/Category:Snow/',]

    def start_requests(self):
        if os.path.exists("res_parse2.json"):
            os.remove("res_parse2.json")
        if os.path.exists("res_parse3.json"):
            os.remove("res_parse3.json")
        if os.path.exists("res_parse4.json"):
            os.remove("res_parse4.json")

        urls = []
        filename = f'DBpedia_entities_in_schema.txt'
        with open(filename, 'r') as f:
            lines = f.readlines()
            for line in lines:
                urls.append(line.strip('\n').split(':')[-1])
        
        # urls = ["Flood"]
        for url in urls[:10]:
            category = url
            url = f'http://dbpedia.org/page/Category:' + url
            yield scrapy.Request(url=url, meta={'category':category}, callback=self.parse)


    def parse(self, response):
        urls = response.xpath('//a[@rev="dct:subject"]/@href').getall()
        names = []
        for url in urls:
            names.append(url[28:])
        # names = response.xpath('//a[@rev="dct:subject"]/text()').extract()
        category = response.meta['category']
        print(category, names)

        for name in names:
            url = "http://dbpedia.org/data/" + name + ".json"
            # ipdb.set_trace()
            yield scrapy.Request(url,meta={'Category':category, 'Subject': name},callback=self.parse2)


    def parse2(self,response):
        category = response.meta['Category']
        subject = response.meta['Subject']
        item = dict()
        item['Category'] = category
        item['Subject'] = subject
        # print("pares2:", item['Category'], item['Subject'])
        dic = response.json()
        abstract = {}
        if 'http://dbpedia.org/ontology/abstract' in dic['http://dbpedia.org/resource/'+subject]:
            abstract = dic['http://dbpedia.org/resource/'+subject]['http://dbpedia.org/ontology/abstract']
        flag = True
        value = ""
        lang = 1
        for key in abstract:
            if key['lang'] == 'zh':
                value = key['value']
                flag = False
                lang = 0
            if flag and key['lang'] == 'en':
                value = key['value']
        item['abstract'] = {'value':value, 'lang':lang}
        if 'http://dbpedia.org/ontology/thumbnail' in dic['http://dbpedia.org/resource/'+subject]:
            item['thumbnail'] = dic['http://dbpedia.org/resource/'+subject]['http://dbpedia.org/ontology/thumbnail'][0]['value']
        else:
            item['thumbnail'] = ""
        
        tmp_subjects = []
        if 'http://purl.org/dc/terms/subject' in dic['http://dbpedia.org/resource/'+subject]:
            subjects = dic['http://dbpedia.org/resource/'+subject]['http://purl.org/dc/terms/subject']
            for key in subjects:
                tmp_subjects.append(key['value'])
        item['subjects'] = tmp_subjects
        
        item['label'] = {'value':"", 'lang':0}
        if 'http://www.w3.org/2000/01/rdf-schema#label' in dic['http://dbpedia.org/resource/'+subject]:
            label = dic['http://dbpedia.org/resource/'+subject]['http://www.w3.org/2000/01/rdf-schema#label']
            flag = True
            value = ""
            lang = 1
            for key in label:
                if key['lang'] == 'zh':
                    value = key['value']
                    flag = False
                    lang = 0
                if flag and key['lang'] == 'en':
                    value = key['value']
            item['label'] = {'value':value, 'lang':lang}

        item['wiki'] = dic['http://dbpedia.org/resource/'+subject]['http://www.w3.org/ns/prov#wasDerivedFrom'][0]['value']

        tmp_url = ""
        if 'http://dbpedia.org/resource/'+subject+'_(disambiguation)' in dic:
            if "http://dbpedia.org/ontology/wikiPageRedirects" not in dic['http://dbpedia.org/resource/'+subject+'_(disambiguation)']:
                tmp_url =  'http://dbpedia.org/resource/'+subject+'_(disambiguation)'
        item['wikiPageDisambiguates'] = tmp_url

        tmp_redic = []
        for x in dic.keys():
            redic = dic[x]
            if "http://dbpedia.org/ontology/wikiPageRedirects" in redic:
                tmp_redic.append(x)
        item['wikiPageRedirects'] =  tmp_redic

        if tmp_url != "":
            tmp_url = tmp_url.replace("resource", "data")
            tmp_url += '.json'
            # ipdb.set_trace()
            yield scrapy.Request(tmp_url, meta={'Category':category, 'Subject': subject}, callback=self.parse3)

        if response.meta['depth'] == 1:
            print("pares2:", item['Category'], item['Subject'])
            filename = f'res_parse2.json'
            json_str = json.dumps(item)
            with open(filename, 'a+') as f:
                f.write(json_str+'\n')
        else:
            print("pares4:", item['Category'], item['Subject'])
            filename = f'res_parse4.json'
            json_str = json.dumps(item)
            with open(filename, 'a+') as f:
                f.write(json_str+'\n')
    
    def parse3(self,response):
        category = response.meta['Category']
        subject = response.meta['Subject']
        item = dict()
        item['Subject'] = subject
        item['Category'] = category
        # print("parse3", response.url, item['Subject'])
        dic = response.json()
        # ipdb.set_trace()

        wikiPageDisambiguates = dic['http://dbpedia.org/resource/'+subject+'_(disambiguation)']['http://dbpedia.org/ontology/wikiPageDisambiguates']
        tmp_wikiPageDisambiguates = []
        for x in wikiPageDisambiguates:
            tmp_wikiPageDisambiguates.append(x['value'])
    
        item['disambiguates'] = tmp_wikiPageDisambiguates
        filename = f'res_parse3.json'
        json_str = json.dumps(item)
        with open(filename, 'a+') as f:
            f.write(json_str+'\n')

        print("parse3", item)

        for url in tmp_wikiPageDisambiguates:
            x = url
            url = url.replace("resource", "data")
            url += '.json'
            yield scrapy.Request(url, meta={'Category':subject, 'Subject': x[28:]}, callback=self.parse2)