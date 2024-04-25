-- ИПУ на договорах юрлиц
--избранные, для экологов

select
  c.CONTRACT_NMBR as "№ договора"
  ,cc8.name as "Наименование юр.лица"
  , rk_ref.name as "Тип объекта"
  ,case when coalesce(t.name,t1.name) is null then 'г. Киров' else coalesce(tt.short_name,tt1.short_name) || ' ' || coalesce(t.name,t1.name) end
  || case when s.NAME is null then '' else ', ' end || st.SHORT_NAME || ' ' || s.NAME || ', д. '||
  h.HOUSE || case when h.CORPUS is not null then ', корп. '|| h.corpus end 
  || case when h.building is not null then ', стр. '|| h.building end 
  || case when na.flat is not null then ', кв. ' || na.flat end as "Полный адрес"

  ,ut.name as "Тип пользователя"
  ,pgCN_Char.fGet_Descr(rk.tu_rlty_kid,
            (
              select cr.CN_CHRCT_ID
                from CN_CHAR_TYPE ct
                  join CN_CHRCTRSTC_GROUP cg on cg.CHAR_TYPE_ID = ct.CHAR_TYPE_ID
                  join CN_CHARACTERISTIC_REF cr on cr.CN_CHRCTRSTC_GROUP_ID = cg.CN_CHRCTRSTC_GROUP_ID
                where ct.NAME = 'Характеристики объекта недвижимости'
                  and cr.CODE = 'NLC846169'
                  and cr.NAME = 'Контролеры'
            ), sysdate) as "Контролер"
  ,cc1.ser_number as "Счетчик (модель)"
  ,cc1.cntr_number as "Серийный № счетчика"
  ,r.Name as "Услуга"
  , case when r.Name = 'Ливневые стоки' then to_number(pgCN_Char.fGet_Descr(rk.tu_rlty_kid,
            (
              select cr.CN_CHRCT_ID
                from CN_CHAR_TYPE ct
                  join CN_CHRCTRSTC_GROUP cg on cg.CHAR_TYPE_ID = ct.CHAR_TYPE_ID
                  join CN_CHARACTERISTIC_REF cr on cr.CN_CHRCTRSTC_GROUP_ID = cg.CN_CHRCTRSTC_GROUP_ID
                where ct.NAME = 'Характеристики объекта недвижимости'
                  and cr.CODE = 'LIVN'
            ), sysdate))
      else
      Fgetindvol(ck.cntr_contract_key_id, cc1.cd_cntr_kid, sk.cntr_service_key_id, bl.bl_bill_id) end as "Объем"
  , (select value from cd_indication where cd_ind_id=pgcd_indication.fGet_LastIndicationID(cc1.cd_cntr_kid)) as "Показания"
  , (select date_ind_take from cd_indication where cd_ind_id=pgcd_indication.fGet_LastIndicationID(cc1.cd_cntr_kid)) as "Дата приема показаний"
  , (select name from cd_indication_source where cd_indic_src_id=(select cd_indic_src_id from cd_indication where cd_ind_id=pgcd_indication.fGet_LastIndicationID(cc1.cd_cntr_kid))) as "Источник показаний"
 
from cn_contract_key ck
join cn_contract c on c.cntr_contract_key_id = ck.cntr_contract_key_id and c.is_active=1 and c.cntr_status_id=5 and sysdate between c.date_begin and c.date_end

join Cn_realty_bunch_key cc on ck.cntr_contract_key_id=cc.cntr_contract_key_id
join TU_REALTY_KEY rk on cc.TU_RLTY_KID=rk.tu_rlty_kid
join tu_realty_species_ref rk_ref on rk.tu_rlty_spcs_id=rk_ref.tu_rlty_spcs_id 

join NS_ADDRESS na on na.NS_ADR_ID = rk.NS_ADR_ID
left join NS_HOUSE h on h.NS_HOS_ID = na.NS_HOS_ID
left join NS_STREET s on s.NS_STRT_ID = h.NS_STRT_ID
left join NS_STREET_TYPE st on st.NS_STRTTYP_ID = s.NS_STRTTYP_ID

left join NS_TOWN t on t.NS_TWN_ID = s.NS_TWN_ID
left join NS_TOWN t1 on t1.NS_TWN_ID = h.NS_TWN_ID
left join ns_town_type tt on t.ns_twntyp_id=tt.ns_twntyp_id
left join ns_town_type tt1 on t1.ns_twntyp_id=tt1.ns_twntyp_id    
left outer join us_flat_account_ver fr on fr.us_flat_account_kid = ck.us_account_id

join cn_service_key sk on c.cntr_contract_key_id = sk.cntr_contract_key_id and sk.tu_rlty_kid=cc.tu_rlty_kid
join US_COUNTERAGENT q on q.US_COUNTERAGENT_ID = ck.US_COUNTERAGENT_ID
join US_CORP_CARD cc8 on cc8.US_CORP_ID = q.US_CORP_ID
join ns_user_type ut on ut.ns_user_typ_id = q.ns_user_typ_id
left join ns_svc_ref r on r.ns_svc_id = sk.ns_svc_id

left join cd_const_service_bunch b on sk.cntr_service_key_id = b.us_cntr_svc_id and b.is_active = 1 and sysdate between b.date_begin and b.date_end
left join cd_counter cc1 on cc1.cd_cntr_kid = b.cd_cntr_kid and cc1.is_active=1
, bl_bill_periods bl

where ck.us_account_id is null
and fr.us_flat_account_kid is null
and (cc1.ser_number is not null or (r.Name='Ливневые стоки' and pgCN_Char.fGet_Descr(rk.tu_rlty_kid,
            (
              select cr.CN_CHRCT_ID
                from CN_CHAR_TYPE ct
                  join CN_CHRCTRSTC_GROUP cg on cg.CHAR_TYPE_ID = ct.CHAR_TYPE_ID
                  join CN_CHARACTERISTIC_REF cr on cr.CN_CHRCTRSTC_GROUP_ID = cg.CN_CHRCTRSTC_GROUP_ID
                where ct.NAME = 'Характеристики объекта недвижимости'
                  and cr.CODE = 'LIVN'
            ), sysdate) is not null))

--and c.contract_nmbr='42-0183' and h.ns_hos_id in (2599443)  ----!!!!!!!!!!!!!!!


and (
  (c.contract_nmbr='43-2204' and h.ns_hos_id in (3644527,2607800,2607805,2608103,3935839)) or
  (c.contract_nmbr='42-0002' and h.ns_hos_id in (3636715)) or
  (c.contract_nmbr='42-5361' and h.ns_hos_id in (2605828)) or
  (c.contract_nmbr='42-0130' and h.ns_hos_id in (3644785)) or
  (c.contract_nmbr='43-1368' and h.ns_hos_id in (3637059)) or
  (c.contract_nmbr='42-6142' and h.ns_hos_id in (2612119,2610172,2612460,2615494)) or
  (c.contract_nmbr='01-138/19-н/42-1808' and h.ns_hos_id in (2612352)) or
  (c.contract_nmbr='01-355/19-н/42-0023' and h.ns_hos_id in (2613635,3377386,2601590,2603649,3636687,2614463)) or
  (c.contract_nmbr='42-0183' and h.ns_hos_id in (2599443)) or
  (c.contract_nmbr='42-0022' and h.ns_hos_id in (3635577,2615510,3179874)) or
  (c.contract_nmbr='03-137/19-н/42-0003' and h.ns_hos_id in (3636761,2616075)) or
  (c.contract_nmbr='42-0031' and h.ns_hos_id in (2607658,3897401)) or
  (c.contract_nmbr='43-1224' and h.ns_hos_id in (3684853)) or
  (c.contract_nmbr='42-0043' and h.ns_hos_id in (2601793,3639227,3637539)) or 
  (c.contract_nmbr='42-0200' and h.ns_hos_id in (3639751,3888698)) or
  (c.contract_nmbr='42-2049' and h.ns_hos_id in (2613657,2601714,2606513,2605216,3636595,2609968,3639299,2610475,3670424,2610482,2610382,2613655,3644957,2612471,2616036)) or
  (c.contract_nmbr='42-4788' and h.ns_hos_id in (3635931)) or
  (c.contract_nmbr='42-5031' and h.ns_hos_id in (3386307)) or
  (c.contract_nmbr='03-133/19-н' and h.ns_hos_id in (3557249,3385969)) or
  (c.contract_nmbr='59/14' and h.ns_hos_id in (3081789)) or
  (c.contract_nmbr='42-1021' and h.ns_hos_id in (2611902)) or
  (c.contract_nmbr='42-0230' and h.ns_hos_id in (2606912,3655743,3655748,3655776,3655785,3655756,3655763,3655760,3655735,3655783,3655695,3655693)) or
  (c.contract_nmbr='42-0054' and h.ns_hos_id in (2601793)) or
  (c.contract_nmbr='42-3974' and h.ns_hos_id in (2606574)) or
  (c.contract_nmbr='42-2553' and h.ns_hos_id in (3645653,2615049)) or
  (c.contract_nmbr='42-1087' and h.ns_hos_id in (2614531)) or
  (c.contract_nmbr='42-7474' and h.ns_hos_id in (2610593)) or
  (c.contract_nmbr='42-4525' and h.ns_hos_id in (3386307,2613917)) or
  (c.contract_nmbr='42-9103' and h.ns_hos_id in (2610970)) or
  (c.contract_nmbr='42-3075' and h.ns_hos_id in (2604201,3267910)) or
  (c.contract_nmbr='42-9959' and h.ns_hos_id in (2613880)) or
  (c.contract_nmbr='42-2052' and h.ns_hos_id in (3683715)) or
  (c.contract_nmbr='42-1361' and h.ns_hos_id in (2614367)) or
  (c.contract_nmbr='42-5588' and h.ns_hos_id in (2603978)) or
  (c.contract_nmbr='42-4137' and h.ns_hos_id in (3894176,2605823,2607582)) or
  (c.contract_nmbr='42-8050' and h.ns_hos_id in (3910519)) or
  (c.contract_nmbr='42-1702' and h.ns_hos_id in (2612890,2606769)) or
  (c.contract_nmbr='42-0505' and h.ns_hos_id in (3646648)) or
  (c.contract_nmbr='42-0262' and h.ns_hos_id in (2613725)) or
  (c.contract_nmbr='42-0518' and h.ns_hos_id in (2615489)) or
  (c.contract_nmbr='01-139/19-н/42-0160' and h.ns_hos_id in (2612452)) or
  (c.contract_nmbr='42-0082' and h.ns_hos_id in (3655598)) or
  (c.contract_nmbr='87/16' and h.ns_hos_id in (3952166)) or
  (c.contract_nmbr='42-0368' and h.ns_hos_id in (3267892,3346199)) or
  (c.contract_nmbr='42-1858' and h.ns_hos_id in (2610384,2614933)) or
  (c.contract_nmbr='42-3765' and h.ns_hos_id in (2615621)) or
  (c.contract_nmbr='42-2268' and h.ns_hos_id in (2608772)) or
  (c.contract_nmbr='42-4882' and h.ns_hos_id in (2601667)) or
  (c.contract_nmbr='42-9016' and h.ns_hos_id in (3422006,2604565)) or
  (c.contract_nmbr='42-9151' and h.ns_hos_id in (3544343,2601261,2599631,2604245,2609541,2611499,2610338,2611112)) or
  (c.contract_nmbr='42-2891' and h.ns_hos_id in (2613362)) or
  (c.contract_nmbr='42-9938' and h.ns_hos_id in (3376928)) or
  (c.contract_nmbr='42-6112' and h.ns_hos_id in (2608955)) or
  (c.contract_nmbr='42-1352' and h.ns_hos_id in (2615956)) or
  (c.contract_nmbr='42-4829' and h.ns_hos_id in (2614427)) or
  (c.contract_nmbr='42-4915' and h.ns_hos_id in (2607895)) or
  (c.contract_nmbr='42-0150' and h.ns_hos_id in (2614147)) or
  (c.contract_nmbr='42-2430' and h.ns_hos_id in (2609836)) or
  (c.contract_nmbr='43-0910' and h.ns_hos_id in (2610536)) or
  (c.contract_nmbr='42-5223' and h.ns_hos_id in (2607281)) or
  (c.contract_nmbr='42-3686' and h.ns_hos_id in (2609813)) or
  (c.contract_nmbr='43-0586' and h.ns_hos_id in (3639807)) or
  (c.contract_nmbr='43-1352' and h.ns_hos_id in (3945439)) or
  (c.contract_nmbr='42-7371' and h.ns_hos_id in (2602585)) or
  (c.contract_nmbr='42-7800' and h.ns_hos_id in (2615641)) or
  (c.contract_nmbr='42-2536' and h.ns_hos_id in (2612926)) or
  (c.contract_nmbr='42-0180' and h.ns_hos_id in (2614367)) or
  (c.contract_nmbr='42-6074' and h.ns_hos_id in (2605247)) or
  (c.contract_nmbr='42-1959' and h.ns_hos_id in (2610333)) or
  (c.contract_nmbr='42-0026' and h.ns_hos_id in (2612899,2600074,2602146,2608594,2608772,3638437,3638315,2613290)) or
  (c.contract_nmbr='01-211/19-н/42-3136' and h.ns_hos_id in (3644241,3638139)) or
  (c.contract_nmbr='42-2596' and h.ns_hos_id in (3644981)) or
  (c.contract_nmbr='43-0536' and h.ns_hos_id in (3268266)) or
  (c.contract_nmbr='42-5892' and h.ns_hos_id in (3645605)) or
  (c.contract_nmbr='42-3006' and h.ns_hos_id in (2612784)) or
  (c.contract_nmbr='42-0176' and h.ns_hos_id in (2614410,2614412,2614575,2609022,2601510,2604634,2604708,2605367,2607359,2607363,2607365,2605007,2606543,3655632,3655729,3655660,2605285,3638385,2609149,2609294,2609294,2609301,2611813,2611813,2610952,2611095,2614674,2614708,3635731,2612850,3655725)) or
  (c.contract_nmbr='42-0332' and h.ns_hos_id in (2604154,2604274,2604274,2604274,2605652,2615508,2610647)) or
  (c.contract_nmbr='42-0403' and h.ns_hos_id in (2612877,2607629,3669158,2606863,2608287,2608289,2611520,2612388)) or
  (c.contract_nmbr='42-1058' and h.ns_hos_id in (2612574)) or 
  (c.contract_nmbr='42-0404' and h.ns_hos_id in (3668877)) or
  (c.contract_nmbr='42-6192' and h.ns_hos_id in (3266136))
)

-- and bl.bl_bill_id = 202401    ------ ПАРАМЕТР ПОИСКА !!!!!!!!!!!!!!!
and bl.bl_bill_id = (select max(bl_bill_id) from bl_bill_periods where period_status='O')  -- period_status='C' - период закрыт; period_status='O' - период еще не закрыт

order by c.CONTRACT_NMBR,ck.cntr_contract_key_id,"Полный адрес"
