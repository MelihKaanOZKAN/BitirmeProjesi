from django import forms
from .models import sentiments
MODES = [
        ('filter', 'Filter')
    ]
METHODES = [
        ('naiveBayes', 'Naive Bayes')
    ]
class sentimentForm(forms.Form):
    sentimentName = forms.CharField(required=True,label='Analysis Name')
    mode = forms.ChoiceField(required=True, widget=forms.RadioSelect, choices=MODES, label='Tweepy Server Mode')
    method_  = forms.ChoiceField(required=True, widget=forms.RadioSelect, choices=METHODES, label='Analysis Method')
    keywords = forms.CharField(required=True, widget=forms.Textarea(attrs={'placeholder': 'separate with comma'}))
    notes = forms.CharField(required=True, widget=forms.Textarea(attrs={'placeholder': 'separate with comma'}))


