CREATE TABLE customer_data (
    fecha_dato DATE,
    ncodpers DOUBLE PRECISION,
    ind_empleado VARCHAR,
    pais_residencia VARCHAR,
    sexo VARCHAR,
    age VARCHAR,
    fecha_alta DATE,
    ind_nuevo VARCHAR,
    antiguedad VARCHAR,
    indrel VARCHAR,
    ult_fec_cli_1t DATE,
    indrel_1mes VARCHAR,
    tiprel_1mes VARCHAR,
    indresi VARCHAR,
    indext VARCHAR,
    conyuemp VARCHAR,
    canal_entrada VARCHAR,
    indfall VARCHAR,
    tipodom VARCHAR,
    cod_prov VARCHAR,
    nomprov VARCHAR,
    ind_actividad_cliente VARCHAR,
    renta DOUBLE PRECISION,
    segmento VARCHAR,
    ind_ahor_fin_ult1 INTEGER,
    ind_aval_fin_ult1 INTEGER,
    ind_cco_fin_ult1 INTEGER,
    ind_cder_fin_ult1 INTEGER,
    ind_cno_fin_ult1 INTEGER,
    ind_ctju_fin_ult1 INTEGER,
    ind_ctma_fin_ult1 INTEGER,
    ind_ctop_fin_ult1 INTEGER,
    ind_ctpp_fin_ult1 INTEGER,
    ind_deco_fin_ult1 INTEGER,
    ind_deme_fin_ult1 INTEGER,
    ind_dela_fin_ult1 INTEGER,
    ind_ecue_fin_ult1 INTEGER,
    ind_fond_fin_ult1 INTEGER,
    ind_hip_fin_ult1 INTEGER,
    ind_plan_fin_ult1 INTEGER,
    ind_pres_fin_ult1 INTEGER,
    ind_reca_fin_ult1 INTEGER,
    ind_tjcr_fin_ult1 INTEGER,
    ind_valo_fin_ult1 INTEGER,
    ind_viv_fin_ult1 INTEGER,
    ind_nomina_ult1 VARCHAR,
    ind_nom_pens_ult1 VARCHAR,
    ind_recibo_ult1 INTEGER
);

ALTER TABLE customer_data RENAME COLUMN fecha_dato TO transaction_date;
ALTER TABLE customer_data RENAME COLUMN ncodpers TO customer_code;
ALTER TABLE customer_data RENAME COLUMN ind_empleado TO employee_index;
ALTER TABLE customer_data RENAME COLUMN pais_residencia TO country_residence;
ALTER TABLE customer_data RENAME COLUMN sexo TO gender;
ALTER TABLE customer_data RENAME COLUMN fecha_alta TO contract_start_date;
ALTER TABLE customer_data RENAME COLUMN ind_nuevo TO new_customer_index;
ALTER TABLE customer_data RENAME COLUMN antiguedad TO customer_seniority;
ALTER TABLE customer_data RENAME COLUMN indrel TO primary_customer_status;
ALTER TABLE customer_data RENAME COLUMN ult_fec_cli_1t TO last_primary_customer_date;
ALTER TABLE customer_data RENAME COLUMN indrel_1mes TO customer_type_start_month;
ALTER TABLE customer_data RENAME COLUMN tiprel_1mes TO customer_relation_type;
ALTER TABLE customer_data RENAME COLUMN indresi TO residence_index;
ALTER TABLE customer_data RENAME COLUMN indext TO foreigner_index;
ALTER TABLE customer_data RENAME COLUMN conyuemp TO spouse_index;
ALTER TABLE customer_data RENAME COLUMN canal_entrada TO customer_join_channel;
ALTER TABLE customer_data RENAME COLUMN indfall TO deceased_index;
ALTER TABLE customer_data RENAME COLUMN tipodom TO address_type;
ALTER TABLE customer_data RENAME COLUMN cod_prov TO province_code;
ALTER TABLE customer_data RENAME COLUMN nomprov TO province_name;
ALTER TABLE customer_data RENAME COLUMN ind_actividad_cliente TO activity_index;
ALTER TABLE customer_data RENAME COLUMN renta TO gross_income;
ALTER TABLE customer_data RENAME COLUMN segmento TO customer_segmentation;
ALTER TABLE customer_data RENAME COLUMN ind_ahor_fin_ult1 TO saving_account;
ALTER TABLE customer_data RENAME COLUMN ind_aval_fin_ult1 TO guarantees;
ALTER TABLE customer_data RENAME COLUMN ind_cco_fin_ult1 TO current_account;
ALTER TABLE customer_data RENAME COLUMN ind_cder_fin_ult1 TO derivada_account;
ALTER TABLE customer_data RENAME COLUMN ind_cno_fin_ult1 TO payroll_account;
ALTER TABLE customer_data RENAME COLUMN ind_ctju_fin_ult1 TO junior_account;
ALTER TABLE customer_data RENAME COLUMN ind_ctma_fin_ult1 TO mas_particular_account;
ALTER TABLE customer_data RENAME COLUMN ind_ctop_fin_ult1 TO particular_account;
ALTER TABLE customer_data RENAME COLUMN ind_ctpp_fin_ult1 TO particular_plus_account;
ALTER TABLE customer_data RENAME COLUMN ind_deco_fin_ult1 TO short_term_deposits;
ALTER TABLE customer_data RENAME COLUMN ind_deme_fin_ult1 TO medium_term_deposits;
ALTER TABLE customer_data RENAME COLUMN ind_dela_fin_ult1 TO long_term_deposits;
ALTER TABLE customer_data RENAME COLUMN ind_ecue_fin_ult1 TO e_account;
ALTER TABLE customer_data RENAME COLUMN ind_fond_fin_ult1 TO investment_funds;
ALTER TABLE customer_data RENAME COLUMN ind_hip_fin_ult1 TO mortgage;
ALTER TABLE customer_data RENAME COLUMN ind_plan_fin_ult1 TO pension_funds;
ALTER TABLE customer_data RENAME COLUMN ind_pres_fin_ult1 TO loans;
ALTER TABLE customer_data RENAME COLUMN ind_reca_fin_ult1 TO taxes;
ALTER TABLE customer_data RENAME COLUMN ind_tjcr_fin_ult1 TO credit_card;
ALTER TABLE customer_data RENAME COLUMN ind_valo_fin_ult1 TO securities;
ALTER TABLE customer_data RENAME COLUMN ind_viv_fin_ult1 TO home_account;
ALTER TABLE customer_data RENAME COLUMN ind_nomina_ult1 TO payroll;
ALTER TABLE customer_data RENAME COLUMN ind_nom_pens_ult1 TO pension_payments;
ALTER TABLE customer_data RENAME COLUMN ind_recibo_ult1 TO direct_debit;



