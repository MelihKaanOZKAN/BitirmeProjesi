class rddCorrector():
    prevRdds = []

    def correct(self, text: str):

        if text.startswith("<tweet>") and text.endswith("</tweet>"):
            return text

        if text == '':
            return ''
        if len(self.prevRdds) > 0:
            start: str = self.prevRdds[0]
            if start.startswith("<tweet>") and text.endswith("</tweet>"):
                self.prevRdds.append(text)
                result = ""
                for i in self.prevRdds:
                    result += i + " "
                self.prevRdds.clear()
                return result
            else:
                self.prevRdds.append(text)
                return ''

        else:
            self.prevRdds.append(text)
            return ''