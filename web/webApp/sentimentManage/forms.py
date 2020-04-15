from django import forms
from .models import sentiments
MODES = [
        ('filter', 'filter')
    ]
class sentimentForm(forms.Form):
    sentimentName = forms.CharField(required=True)
    mode = forms.ChoiceField(required=True, widget=forms.RadioSelect, choices=MODES)
    keywords = forms.CharField(required=True, widget=forms.Textarea(attrs={'placeholder': 'separete with comma'}))
    notes = forms.CharField(required=True, widget=forms.Textarea(attrs={'placeholder': 'separete with comma'}))


