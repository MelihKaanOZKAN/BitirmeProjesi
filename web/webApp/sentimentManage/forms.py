from django import forms
from .models import sentiments
MODES = [
        ('filter', 'Filter')
    ]
METHODES = [
        ('naiveBayes', 'Naive Bayes')
    ]
class sentimentForm(forms.Form):
    sentimentName = forms.CharField(required=True)
    mode = forms.ChoiceField(required=True, widget=forms.RadioSelect, choices=MODES)
    method_  = forms.ChoiceField(required=True, widget=forms.RadioSelect, choices=METHODES)
    keywords = forms.CharField(required=True, widget=forms.Textarea(attrs={'placeholder': 'separate with comma'}))
    notes = forms.CharField(required=True, widget=forms.Textarea(attrs={'placeholder': 'separate with comma'}))


