from django.db import models


class KLine(models.Model):

    id = models.BigIntegerField(primary_key=True)
    name = models.CharField(max_length=20)
    k_date = models.DateField()
    close = models.FloatField()
    volume = models.FloatField()
    open = models.FloatField()
    high = models.FloatField()
    low = models.FloatField()

    class Meta:
        db_table = 'k_line'
