select
  lic as ls, fio,close_ls
  ,Replace(IIF(d1.name='', 'Киров г', 'Киров г ('||d1.name||')') || ', ул. ' || 
                           a1.name || ', д. ' || b1.dom || iif(char_length(b1.dom2)>0,b1.dom2,'') || 
                           iif(char_length(b1.korp)>0,' корп. ' || b1.korp,'')
  || IIF(a.kv=0,'',', кв. '||a.kv) || IIF(a.kv2 is null, '',a.kv2)
  ,'Киров г','г. Киров') as address
from nanim as a
  left join dom as b1 on b1.id=a.adres_id
  left join ulicy as a1 on a1.id=b1.ul_id
  left join city as d1 on a1.city_id=d1.id
where data_my=(select max(data_my) from nanim);
