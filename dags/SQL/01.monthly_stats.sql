-- Ежемесячная статистика с учетом нулевого тарифа

select 'Отчет-статистика' as "Параметр", (select bill_name from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) as "Значение" from dual
union all

select '''====== Общая статистика ======' as "1", '' as "2" from dual
union all
select 'Открытых ЛС всего',
to_char(count(*))
from US_FLAT_ACCOUNT_ver 
where is_active=1 
and us_baseclose_doc_id is null 
and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between date_begin and date_end

union all

select 'Действующих договоров всего',
to_char(count(*))
from CN_CONTRACT_KEY ck
join cn_contract c on c.cntr_contract_key_id = ck.cntr_contract_key_id 
join US_COUNTERAGENT q on q.US_COUNTERAGENT_ID = ck.US_COUNTERAGENT_ID
join ns_user_type ut on ut.ns_user_typ_id = q.ns_user_typ_id
  
where c.IS_ACTIVE = 1
and c.cntr_status_id=5
and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between c.date_begin and c.date_end
and ut.name <> 'Физическое лицо'

union all
select '''====== Переданные показания счетчиков через ЛК ======' as "1", '' as "2" from dual
union all
select 'ФЛ (кол-во ЛС) (Всего, через все каналы)', to_char(count(num_account)) num_accounts
from (
  select distinct
  bp.bl_bill_id,
  fa.NUM_ACCOUNT

from US_FLAT_ACCOUNT_ver fa
  join bl_bill_periods bp on bp.bill_date between fa.date_begin and fa.date_end
  left join cn_contract_key ck on fa.us_flat_account_kid = ck.us_account_id
    left join cn_service_key sk on ck.cntr_contract_key_id = sk.cntr_contract_key_id
  join cd_const_service_bunch b on sk.cntr_service_key_id = b.us_cntr_svc_id and b.is_active = 1 and bp.bill_date between b.date_begin and b.date_end
  join (
  select ci1.cd_cntr_kid, ci1.bl_bill_id, ci1.date_ind_take,ci1.cd_indic_src_id,ci1.value
  from cd_indication ci1
  join (select bl_bill_id,cd_cntr_kid,max(date_ind_take) as date_ind_take,max(value) as value from cd_indication group by bl_bill_id,cd_cntr_kid) a14 on ci1.cd_cntr_kid=a14.cd_cntr_kid and ci1.bl_bill_id=a14.bl_bill_id and a14.date_ind_take=ci1.date_ind_take and a14.value=ci1.value
  ) ci on b.cd_cntr_kid=ci.cd_cntr_kid and ci.bl_bill_id=bp.bl_bill_id

  join cd_indication_source src on src.cd_indic_src_id = ci.cd_indic_src_id

  where fa.IS_ACTIVE = '1'
    and fa.US_BASECLOSE_DOC_ID is NULL
    and src.name in ('Личный кабинет РКЦ','Заведено через ЛК','Телеграмм-бот РКЦ','Голосовой бот РКЦ')
)
where bl_bill_id in (select bl_bill_id from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C'))

union all

select 'ФЛ, '||name,to_char(count(NUM_ACCOUNT)) from (
  select distinct
  src.name,
  fa.NUM_ACCOUNT

from US_FLAT_ACCOUNT_ver fa
  join bl_bill_periods bp on bp.bill_date between fa.date_begin and fa.date_end
  left join cn_contract_key ck on fa.us_flat_account_kid = ck.us_account_id
    left join cn_service_key sk on ck.cntr_contract_key_id = sk.cntr_contract_key_id
  join cd_const_service_bunch b on sk.cntr_service_key_id = b.us_cntr_svc_id and b.is_active = 1 and bp.bill_date between b.date_begin and b.date_end
  join (
  select ci1.cd_cntr_kid, ci1.bl_bill_id, ci1.date_ind_take,ci1.cd_indic_src_id,ci1.value
  from cd_indication ci1
  join (select bl_bill_id,cd_cntr_kid,max(date_ind_take) as date_ind_take,max(value) as value from cd_indication group by bl_bill_id,cd_cntr_kid) a14 on ci1.cd_cntr_kid=a14.cd_cntr_kid and ci1.bl_bill_id=a14.bl_bill_id and a14.date_ind_take=ci1.date_ind_take and a14.value=ci1.value
  ) ci on b.cd_cntr_kid=ci.cd_cntr_kid and ci.bl_bill_id=bp.bl_bill_id

  join cd_indication_source src on src.cd_indic_src_id = ci.cd_indic_src_id

  where fa.IS_ACTIVE = '1'
    and fa.US_BASECLOSE_DOC_ID is NULL
    and src.name in ('Личный кабинет РКЦ','Заведено через ЛК','Телеграмм-бот РКЦ','Голосовой бот РКЦ')
    and bp.bl_bill_id in (select bl_bill_id from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C'))
) group by name

union all

select 'ЮЛ (кол-во договоров)',
to_char(count(CONTRACT_NMBR))
from (
  select distinct
  bp.bl_bill_id
  ,c.CONTRACT_NMBR
  from cn_contract_key ck
  join cn_contract c on c.cntr_contract_key_id = ck.cntr_contract_key_id
  join bl_bill_periods bp on bp.bill_date between c.date_begin and c.date_end
  join Cn_realty_bunch_key cc on ck.cntr_contract_key_id=cc.cntr_contract_key_id
  join TU_REALTY_KEY rk on cc.TU_RLTY_KID=rk.tu_rlty_kid
  join TU_REALTY tr on rk.tu_rlty_kid = tr.tu_rlty_kid and tr.is_active=1 and bp.bill_date between tr.date_begin and tr.date_end
  join cn_service_key sk on c.cntr_contract_key_id = sk.cntr_contract_key_id and sk.tu_rlty_kid=cc.tu_rlty_kid
  join cd_const_service_bunch b on sk.cntr_service_key_id = b.us_cntr_svc_id and b.is_active = 1 and bp.bill_date between b.date_begin and b.date_end
  join (
  select ci1.cd_cntr_kid, ci1.bl_bill_id, ci1.date_ind_take,ci1.cd_indic_src_id,ci1.value
  from cd_indication ci1
  join (select bl_bill_id,cd_cntr_kid,max(date_ind_take) as date_ind_take,max(value) as value from cd_indication group by bl_bill_id,cd_cntr_kid) a14 on ci1.cd_cntr_kid=a14.cd_cntr_kid and ci1.bl_bill_id=a14.bl_bill_id and a14.date_ind_take=ci1.date_ind_take and a14.value=ci1.value
  ) ci on b.cd_cntr_kid=ci.cd_cntr_kid and ci.bl_bill_id=bp.bl_bill_id
  join cd_indication_source src on src.cd_indic_src_id = ci.cd_indic_src_id
  where ck.us_account_id is null
      and c.is_active= 1
      and c.cntr_status_id = 5
      and src.name in ('Личный кабинет РКЦ')
)
where bl_bill_id in (select bl_bill_id from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C'))

union all 
select '''====== Статистика (Дома) ======' as "1", '' as "2" from dual
union all

select 'Количество МКД', to_char(count(*)) from (
  select distinct na.NS_HOS_ID
  from US_FLAT_ACCOUNT_ver fa
    join TU_REALTY_KEY rk on rk.TU_RLTY_KID = fa.TU_RLTY_KID
    join tu_realty_species_ref rk_ref on rk.tu_rlty_spcs_id=rk_ref.tu_rlty_spcs_id 
    join NS_ADDRESS na on na.NS_ADR_ID = rk.NS_ADR_ID

  where fa.IS_ACTIVE = '1'
      and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between fa.DATE_BEGIN and fa.DATE_END
      and fa.US_BASECLOSE_DOC_ID is NULL
  --    and fa.num_account in ('3081700', '3340983')
      and rk_ref.name in ('Квартира в МКД', 'Комната', 'Комната в общежитии', 'МКД')
  )

union all

select 'Количество МКД со счетчиками ХВС', to_char(count(*)) from (
  select distinct na.NS_HOS_ID

  from US_FLAT_ACCOUNT_ver fa
    join TU_REALTY_KEY rk on rk.TU_RLTY_KID = fa.TU_RLTY_KID
    join tu_realty_species_ref rk_ref on rk.tu_rlty_spcs_id=rk_ref.tu_rlty_spcs_id 
    join NS_ADDRESS na on na.NS_ADR_ID = rk.NS_ADR_ID
    join cn_contract_key ck on fa.us_flat_account_kid = ck.us_account_id
    join cn_service_key sk on ck.cntr_contract_key_id = sk.cntr_contract_key_id
    join ns_svc_ref r on r.ns_svc_id = sk.ns_svc_id
    join cd_const_service_bunch b on sk.cntr_service_key_id = b.us_cntr_svc_id and b.is_active = 1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between b.date_begin and b.date_end
  where fa.IS_ACTIVE = '1'
      and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between fa.DATE_BEGIN and fa.DATE_END
      and fa.US_BASECLOSE_DOC_ID is NULL
      and rk_ref.name in ('Квартира в МКД', 'Комната', 'Комната в общежитии', 'МКД')
      and (r.name like 'Водоснабжение%')
)

union all

select 'Количество МКД со счетчиками ГВС', to_char(count(*)) from (
  select distinct na.NS_HOS_ID

  from US_FLAT_ACCOUNT_ver fa
    join TU_REALTY_KEY rk on rk.TU_RLTY_KID = fa.TU_RLTY_KID
    join tu_realty_species_ref rk_ref on rk.tu_rlty_spcs_id=rk_ref.tu_rlty_spcs_id 
    join NS_ADDRESS na on na.NS_ADR_ID = rk.NS_ADR_ID
    
    join cn_contract_key ck on fa.us_flat_account_kid = ck.us_account_id
    join cn_service_key sk on ck.cntr_contract_key_id = sk.cntr_contract_key_id
    join ns_svc_ref r on r.ns_svc_id = sk.ns_svc_id

    join cd_const_service_bunch b on sk.cntr_service_key_id = b.us_cntr_svc_id and b.is_active = 1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between b.date_begin and b.date_end


  where fa.IS_ACTIVE = '1'
      and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between fa.DATE_BEGIN and fa.DATE_END
      and fa.US_BASECLOSE_DOC_ID is NULL
      and rk_ref.name in ('Квартира в МКД', 'Комната', 'Комната в общежитии', 'МКД')
      and (r.name like 'ГВС%' or r.name like 'Горячее водоснабжение%' or r.name like 'Холодная вода для ГВС%' or r.name like 'Холодная вода для приготовления горячей%')

)

union all

select 'Количество частных домов', to_char(count(*)) from (
  select distinct na.NS_HOS_ID
  from US_FLAT_ACCOUNT_ver fa
    join TU_REALTY_KEY rk on rk.TU_RLTY_KID = fa.TU_RLTY_KID
    join tu_realty_species_ref rk_ref on rk.tu_rlty_spcs_id=rk_ref.tu_rlty_spcs_id 
    join NS_ADDRESS na on na.NS_ADR_ID = rk.NS_ADR_ID

  where fa.IS_ACTIVE = '1'
      and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between fa.DATE_BEGIN and fa.DATE_END
      and fa.US_BASECLOSE_DOC_ID is NULL
      and rk_ref.name in ('Частный дом', 'Квартира в ЧД')
  )

union all

select 'Количество частных домов со счетчиками ХВС', to_char(count(*)) from (
  select distinct na.NS_HOS_ID

  from US_FLAT_ACCOUNT_ver fa
    join TU_REALTY_KEY rk on rk.TU_RLTY_KID = fa.TU_RLTY_KID
    join tu_realty_species_ref rk_ref on rk.tu_rlty_spcs_id=rk_ref.tu_rlty_spcs_id 
    join NS_ADDRESS na on na.NS_ADR_ID = rk.NS_ADR_ID
    join cn_contract_key ck on fa.us_flat_account_kid = ck.us_account_id
    join cn_service_key sk on ck.cntr_contract_key_id = sk.cntr_contract_key_id
    join ns_svc_ref r on r.ns_svc_id = sk.ns_svc_id
    join cd_const_service_bunch b on sk.cntr_service_key_id = b.us_cntr_svc_id and b.is_active = 1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between b.date_begin and b.date_end
  where fa.IS_ACTIVE = '1'
      and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between fa.DATE_BEGIN and fa.DATE_END
      and fa.US_BASECLOSE_DOC_ID is NULL
      and rk_ref.name in ('Частный дом', 'Квартира в ЧД')
      and (r.name like 'Водоснабжение%')
)

union all

select 'Количество частных домов со счетчиками ГВС', to_char(count(*)) from (
  select distinct na.NS_HOS_ID

  from US_FLAT_ACCOUNT_ver fa
    join TU_REALTY_KEY rk on rk.TU_RLTY_KID = fa.TU_RLTY_KID
    join tu_realty_species_ref rk_ref on rk.tu_rlty_spcs_id=rk_ref.tu_rlty_spcs_id 
    join NS_ADDRESS na on na.NS_ADR_ID = rk.NS_ADR_ID
    
    join cn_contract_key ck on fa.us_flat_account_kid = ck.us_account_id
    join cn_service_key sk on ck.cntr_contract_key_id = sk.cntr_contract_key_id
    join ns_svc_ref r on r.ns_svc_id = sk.ns_svc_id

    join cd_const_service_bunch b on sk.cntr_service_key_id = b.us_cntr_svc_id and b.is_active = 1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between b.date_begin and b.date_end


  where fa.IS_ACTIVE = '1'
      and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between fa.DATE_BEGIN and fa.DATE_END
      and fa.US_BASECLOSE_DOC_ID is NULL
      and rk_ref.name in ('Частный дом', 'Квартира в ЧД')
      and (r.name like 'ГВС%' or r.name like 'Горячее водоснабжение%' or r.name like 'Холодная вода для ГВС%' or r.name like 'Холодная вода для приготовления горячей%')

)

union all 

select '''====== Статистика (ИПУ) ======' as "1", '' as "2" from dual
union all 
select '''---=== ХВС ===---' as "1", '' as "2" from dual
union all 

select 'Количество счетчиков ХВС', to_char(count(*)) from (
  select
    fa.NUM_ACCOUNT
   
    from US_FLAT_ACCOUNT_ver fa
    join cn_contract_key ck on fa.us_flat_account_kid = ck.us_account_id
    join cn_service_key sk on ck.cntr_contract_key_id = sk.cntr_contract_key_id
    join ns_svc_ref r on r.ns_svc_id = sk.ns_svc_id
    join cd_const_service_bunch b on sk.cntr_service_key_id = b.us_cntr_svc_id and b.is_active = 1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between b.date_begin and b.date_end
    inner join cd_counter cc on cc.cd_cntr_kid = b.cd_cntr_kid
    join cn_service s on ck.cntr_contract_key_id = s.cntr_contract_key_id and s.is_active=1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between s.date_begin and s.date_end and s.cntr_service_key_id=sk.cntr_service_key_id
    join ns_tariff_plan tp on s.ns_tariff_plan_id = tp.ns_tariff_plan_id
    where fa.IS_ACTIVE = '1'
      and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between fa.DATE_BEGIN and fa.DATE_END
      and fa.US_BASECLOSE_DOC_ID is NULL
      and cc.is_active=1
      and (r.Name like 'Водоснабжение%')  -- только счетчики ХВС
      and tp.name not like '%улев%'
)

union all
select 'Количество счетчиков ХВС в МКД', to_char(count(*)) from (
  select
    fa.NUM_ACCOUNT
    from US_FLAT_ACCOUNT_ver fa
    join TU_REALTY_KEY rk on rk.TU_RLTY_KID = fa.TU_RLTY_KID
    join tu_realty_species_ref rk_ref on rk.tu_rlty_spcs_id=rk_ref.tu_rlty_spcs_id 
    join cn_contract_key ck on fa.us_flat_account_kid = ck.us_account_id
    join cn_service_key sk on ck.cntr_contract_key_id = sk.cntr_contract_key_id
    join ns_svc_ref r on r.ns_svc_id = sk.ns_svc_id
    join cd_const_service_bunch b on sk.cntr_service_key_id = b.us_cntr_svc_id and b.is_active = 1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between b.date_begin and b.date_end
    inner join cd_counter cc on cc.cd_cntr_kid = b.cd_cntr_kid
    join cn_service s on ck.cntr_contract_key_id = s.cntr_contract_key_id and s.is_active=1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between s.date_begin and s.date_end and s.cntr_service_key_id=sk.cntr_service_key_id
    join ns_tariff_plan tp on s.ns_tariff_plan_id = tp.ns_tariff_plan_id
    where fa.IS_ACTIVE = '1'
      and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between fa.DATE_BEGIN and fa.DATE_END
      and fa.US_BASECLOSE_DOC_ID is NULL
      and cc.is_active=1
      and (r.Name like 'Водоснабжение%')  -- только счетчики ХВС
      and rk_ref.name in ('Квартира в МКД', 'Комната', 'Комната в общежитии', 'МКД')
      and tp.name not like '%улев%'
)


union all
select 'Количество ЛС со счетчиками ХВС', to_char(count(*)) from (
  select distinct 
    fa.NUM_ACCOUNT, r.name
    from US_FLAT_ACCOUNT_ver fa
    join cn_contract_key ck on fa.us_flat_account_kid = ck.us_account_id
    join cn_service_key sk on ck.cntr_contract_key_id = sk.cntr_contract_key_id
    join ns_svc_ref r on r.ns_svc_id = sk.ns_svc_id
    join cn_service s on ck.cntr_contract_key_id = s.cntr_contract_key_id and s.is_active=1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between s.date_begin and s.date_end and s.cntr_service_key_id=sk.cntr_service_key_id
    join cd_const_service_bunch b on sk.cntr_service_key_id = b.us_cntr_svc_id and b.is_active = 1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between b.date_begin and b.date_end
    inner join cd_counter cc on cc.cd_cntr_kid = b.cd_cntr_kid
    join ns_tariff_plan tp on s.ns_tariff_plan_id = tp.ns_tariff_plan_id
    where fa.IS_ACTIVE = '1'
      and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between fa.DATE_BEGIN and fa.DATE_END
      and fa.US_BASECLOSE_DOC_ID is NULL
      and cc.is_active=1
      and (r.Name like 'Водоснабжение%')  -- только счетчики ХВС
      and tp.name not like '%улев%'
)
union all
select 'Количество ЛС услугой ХВС', to_char(count(*)) from (
  select distinct 
    fa.NUM_ACCOUNT
    from US_FLAT_ACCOUNT_ver fa
    join cn_contract_key ck on fa.us_flat_account_kid = ck.us_account_id
    join cn_service_key sk on ck.cntr_contract_key_id = sk.cntr_contract_key_id
    join ns_svc_ref r on r.ns_svc_id = sk.ns_svc_id
    join cn_service s on ck.cntr_contract_key_id = s.cntr_contract_key_id and s.is_active=1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between s.date_begin and s.date_end and s.cntr_service_key_id=sk.cntr_service_key_id
    join ns_tariff_plan tp on s.ns_tariff_plan_id = tp.ns_tariff_plan_id
    
    where fa.IS_ACTIVE = '1'
      and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between fa.DATE_BEGIN and fa.DATE_END
      and fa.US_BASECLOSE_DOC_ID is NULL
      and (r.Name like 'Водоснабжение%')  -- только ХВС
      and tp.name not like '%улев%'
)
union all 
select '''---=== ГВС ===---' as "1", '' as "2" from dual
union all 

select 'Количество счетчиков ГВС', to_char(count(*)) from (
  select
    fa.NUM_ACCOUNT
    from US_FLAT_ACCOUNT_ver fa
    join cn_contract_key ck on fa.us_flat_account_kid = ck.us_account_id
    join cn_service_key sk on ck.cntr_contract_key_id = sk.cntr_contract_key_id
    join ns_svc_ref r on r.ns_svc_id = sk.ns_svc_id
    join cd_const_service_bunch b on sk.cntr_service_key_id = b.us_cntr_svc_id and b.is_active = 1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between b.date_begin and b.date_end
    inner join cd_counter cc on cc.cd_cntr_kid = b.cd_cntr_kid
    join cn_service s on ck.cntr_contract_key_id = s.cntr_contract_key_id and s.is_active=1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between s.date_begin and s.date_end and s.cntr_service_key_id=sk.cntr_service_key_id
    join ns_tariff_plan tp on s.ns_tariff_plan_id = tp.ns_tariff_plan_id
    where fa.IS_ACTIVE = '1'
      and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between fa.DATE_BEGIN and fa.DATE_END
      and fa.US_BASECLOSE_DOC_ID is NULL
      and cc.is_active=1
      and (r.Name like '%ГВС%' or r.Name like '%Горяч%')  -- только счетчики ГВС
      and tp.name not like '%улев%'
)
union all
select 'Количество счетчиков ГВС в МКД', to_char(count(*)) from (
  select
    fa.NUM_ACCOUNT
    from US_FLAT_ACCOUNT_ver fa
    join TU_REALTY_KEY rk on rk.TU_RLTY_KID = fa.TU_RLTY_KID
    join tu_realty_species_ref rk_ref on rk.tu_rlty_spcs_id=rk_ref.tu_rlty_spcs_id 
    join cn_contract_key ck on fa.us_flat_account_kid = ck.us_account_id
    join cn_service_key sk on ck.cntr_contract_key_id = sk.cntr_contract_key_id
    join ns_svc_ref r on r.ns_svc_id = sk.ns_svc_id
    join cd_const_service_bunch b on sk.cntr_service_key_id = b.us_cntr_svc_id and b.is_active = 1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between b.date_begin and b.date_end
    inner join cd_counter cc on cc.cd_cntr_kid = b.cd_cntr_kid
    join cn_service s on ck.cntr_contract_key_id = s.cntr_contract_key_id and s.is_active=1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between s.date_begin and s.date_end and s.cntr_service_key_id=sk.cntr_service_key_id
    join ns_tariff_plan tp on s.ns_tariff_plan_id = tp.ns_tariff_plan_id
    where fa.IS_ACTIVE = '1'
      and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between fa.DATE_BEGIN and fa.DATE_END
      and fa.US_BASECLOSE_DOC_ID is NULL
      and cc.is_active=1
      and (r.Name like '%ГВС%' or r.Name like '%Горяч%')  -- только счетчики ГВС
      and rk_ref.name in ('Квартира в МКД', 'Комната', 'Комната в общежитии', 'МКД')
      and tp.name not like '%улев%'
)

union all
select 'Количество ЛС со счетчиками ГВС', to_char(count(*)) from (
  select distinct 
    fa.NUM_ACCOUNT, r.name
    from US_FLAT_ACCOUNT_ver fa
    join cn_contract_key ck on fa.us_flat_account_kid = ck.us_account_id
    join cn_service_key sk on ck.cntr_contract_key_id = sk.cntr_contract_key_id
    join ns_svc_ref r on r.ns_svc_id = sk.ns_svc_id
    join cd_const_service_bunch b on sk.cntr_service_key_id = b.us_cntr_svc_id and b.is_active = 1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between b.date_begin and b.date_end
    join cn_service s on ck.cntr_contract_key_id = s.cntr_contract_key_id and s.is_active=1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between s.date_begin and s.date_end and s.cntr_service_key_id=sk.cntr_service_key_id
    join ns_tariff_plan tp on s.ns_tariff_plan_id = tp.ns_tariff_plan_id
    inner join cd_counter cc on cc.cd_cntr_kid = b.cd_cntr_kid
    where fa.IS_ACTIVE = '1'
      and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between fa.DATE_BEGIN and fa.DATE_END
      and fa.US_BASECLOSE_DOC_ID is NULL
      and cc.is_active=1
      and (r.Name like '%ГВС%' or r.Name like '%Горяч%')  -- только счетчики ГВС
      and tp.name not like '%улев%'
)

union all
select 'Количество ЛС услугой ГВС', to_char(count(*)) from (
  select distinct 
    fa.NUM_ACCOUNT
    from US_FLAT_ACCOUNT_ver fa
    join cn_contract_key ck on fa.us_flat_account_kid = ck.us_account_id
    join cn_service_key sk on ck.cntr_contract_key_id = sk.cntr_contract_key_id
    join ns_svc_ref r on r.ns_svc_id = sk.ns_svc_id
    join cn_service s on ck.cntr_contract_key_id = s.cntr_contract_key_id and s.is_active=1 and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between s.date_begin and s.date_end and s.cntr_service_key_id=sk.cntr_service_key_id
    join ns_tariff_plan tp on s.ns_tariff_plan_id = tp.ns_tariff_plan_id

    where fa.IS_ACTIVE = '1'
      and (select bill_date from bl_bill_periods where bl_bill_id=(select max(bl_bill_id) from bl_bill_periods where period_status='C')) between fa.DATE_BEGIN and fa.DATE_END
      and fa.US_BASECLOSE_DOC_ID is NULL
      and (r.Name like '%ГВС%' or r.Name like '%Горяч%')  -- только ГВС
      and tp.name not like '%улев%'
)
