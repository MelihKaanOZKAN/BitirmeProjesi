from django.test import TestCase
from django.urls import reverse
from .forms import *
class reportTest(TestCase):


    def test_form_withdata(self):
        form = reportForm(data={
            'reportname':'test',
            'reporttype':'text'
        })
        self.assertTrue(form.is_valid())


