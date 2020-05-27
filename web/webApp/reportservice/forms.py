from django import forms

MODES = [
        ('text','Text'),
    ("pie", "Pie Chart")
    ]

class reportForm(forms.Form):
    reportname = forms.CharField(required=True, label='Report Name')
    reporttype  = forms.ChoiceField(required=True, widget=forms.RadioSelect, choices=MODES, label='Report Type')