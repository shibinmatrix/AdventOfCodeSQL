with data1 as
    (select index as rowid, value as val
     from table(split_to_table('l
l
vqb

zrbnqykcsxjm
rszkqdmjbnx
ynkosrxjbzqm

jiare
eria
iaer
arie
aiexqr','\n\n'))), data as (
select rowid, max(index) over (partition by rowid) as indx, value as val from data1, lateral split_to_table(data1.val,'\n')),
alpha_ser as (select ch from
(select char(seq4()) as ch
from table(generator (rowcount => 200)))
where ch between 'a' and 'z')
select count(*) from
(select rowid,ch, max(indx) as inx, count(*) as cnt from data, alpha_ser
where contains(val, ch)
group by rowid,ch having inx = cnt);
