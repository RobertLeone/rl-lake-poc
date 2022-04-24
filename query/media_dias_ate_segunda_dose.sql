select
    round(avg(date_count),2) avg_days
from
    (select distinct 
        paciente_id,
        date_diff('day', min(vacina_dataaplicacao) over (partition by paciente_id),
        max(vacina_dataaplicacao) over (partition by paciente_id)) date_count
    from datasus_silver_db.datasus
    where {{vacina_dataaplicacao}} and vacina_descricao_dose in ('1° Dose','1ª Dose', 'Dose', 'Única', '2° Dose','2ª Dose') [[AND {{paciente_endereco_uf}}]])
where date_count > 0;