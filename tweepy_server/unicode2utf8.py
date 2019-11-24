

class unicode2utf8():
    def convertJson(self):
        tmpData = None
        with open("tweets_sample.json", "r") as f:
            t = f.read()
            print(t)
            tmpData = self.convert(t)

        with open("tweets_sample_encoded.json", "a", encoding="utf-8") as  k:
            k.write(tmpData)
            
        
    def convert(self, input):
        input_ = str(input).replace("\\u011f",  "ğ")
        input_ = str(input_).replace("\\u011e",  "Ğ")
        input_ = str(input_).replace("\\u0131",  "ı")
        input_ = str(input_).replace("\\u0130",  "İ")
        input_ = str(input_).replace("\\u00f6",  "ö")
        input_ = str(input_).replace("\\u00d6",  "Ö")
        input_ = str(input_).replace("\\u00fc",  "ü")
        input_ = str(input_).replace("\\u00dc",  "Ü")
        input_ = str(input_).replace("\\u015f",  "ş")
        input_ = str(input_).replace("\\u015e",  "Ş")
        input_ = str(input_).replace("\\u00e7",  "ç")
        input_ = str(input_).replace("\\u00c7",  "Ç")
        input_ = str(input_).replace("\\n"," ")
        k = 0
        while(k != -1):
            tmp = input_.find("\\u")
            input_ = input_[0:tmp] + input_[tmp + 6:]
            k=input_.find("\\u")
        return input_ 
