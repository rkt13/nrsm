DROP TABLE IF EXISTS MAT_QIP_RUN;
CREATE LOCAL TEMPORARY TABLE MAT_QIP_RUN
ON COMMIT PRESERVE ROWS AS
SELECT
  '2019-01-01'::TIMESTAMP AS START_DATE,
  '2021-12-31'::TIMESTAMP AS END_DATE
;

-- Relevant codes for analysis
DROP TABLE IF EXISTS MAT_QIP_CODES;
CREATE LOCAL TEMPORARY TABLE MAT_QIP_CODES (
  code_type CHAR(4),
  code_value CHAR(255)
)
ON COMMIT PRESERVE ROWS
;

INSERT INTO MAT_QIP_CODES VALUES ('DRG', '0539'); --C-Section w. Sterilization
INSERT INTO MAT_QIP_CODES VALUES ('DRG', '0540'); --C-Section
INSERT INTO MAT_QIP_CODES VALUES ('DRG', '0541'); --Vaginal delivery w. Sterilization
INSERT INTO MAT_QIP_CODES VALUES ('DRG', '0542'); --Vaginal delivery w. OR Procedure NOT Sterilization
INSERT INTO MAT_QIP_CODES VALUES ('DRG', '0560'); --Vaginal delivery DRG
INSERT INTO MAT_QIP_CODES VALUES ('PROC', '59400'); -- Vaginal delivery global CPT
INSERT INTO MAT_QIP_CODES VALUES ('PROC', '59610'); -- Vaginal delivery gloabl CPT (following cesarean)
COMMIT;

-- Preventable C-Section Logic
-- ===============================================
-- ===============================================

-- Exclude from denominator
-- (1.) Specific diagnosis codes indicative of
--      multiple gestation, complications, previous cesarean,
--      gestational age too low
--
-- (2.) Members w. previous births on record
--      (not nulliparous)

-- Diagnosis code exclusions
DROP TABLE IF EXISTS MAT_QIP_QM_EXCL_DENOM_TCN;
CREATE LOCAL TEMPORARY TABLE MAT_QIP_QM_EXCL_DENOM_TCN
ON COMMIT PRESERVE ROWS AS
SELECT DISTINCT
  tcn
FROM medicaid.claim_trans
WHERE
  latest_trans_ind = 'Y' AND
  claim_trans_id IN (
    SELECT
      claim_trans_id
    FROM medicaid.claim_dx
    WHERE
      srv_dt BETWEEN (SELECT START_DATE FROM MAT_QIP_RUN) AND (SELECT END_DATE FROM MAT_QIP_RUN) AND
      (
        -- Multiple gestation
        SUBSTR(dx_cd,1,3) in ('O30') OR 
        -- Complications and previous cesarean delivery
        SUBSTR(dx_cd,1,4) IN  ('O311', 'O312', 'O318', 'O321', 'O322', 'O323', 'O328', 'O329', 'O364', 'O601', 'O632', 'O641', 'O642', 'O643', 'O644', 'O648', 'O649', 'O661', 'O666', 'Z371', 'Z372', 'Z373', 'Z374', 'Z377') OR
        SUBSTR(dx_cd,1,5) IN ('O4403', 'O4413', 'O4423', 'O4433', 'Z3750', 'Z3751', 'Z3752', 'Z3753', 'Z3754', 'Z3759', 'Z3760', 'Z3761', 'Z3762', 'Z3763', 'Z3764', 'Z3769', 'O3421', 'O6641', 'O3422') OR
        -- Gestational age too low
        SUBSTR(dx_cd,1,4) in ('Z3A0', 'Z3A1', 'Z3A2') OR
        dx_cd in ('Z3A30', 'Z3A31', 'Z3A32', 'Z3A33', 'Z3A34', 'Z3A35', 'Z3A36')
      )
  )
;

-- Inpatient Providers
-- ===============================================
-- ===============================================
DROP TABLE IF EXISTS MAT_PROV;
CREATE LOCAL TEMPORARY TABLE MAT_PROV
ON COMMIT PRESERVE ROWS AS
SELECT
  claim_trans_id,
  claim_trans_src_cd,
  mbr_id,
  bill_npi,
  dsch_dt,
  apr_drg,
  CASE WHEN MAT_QIP_QM_EXCL_DENOM_TCN.tcn IS NOT NULL THEN 1 ELSE 0 END AS EXCL_QM_DENOM,
  CASE WHEN dsch_dt = MIN(dsch_dt) OVER (PARTITION BY mbr_id) THEN 1 ELSE 0 END AS FIRST_BIRTH_IND,
  net_pd_amt_without_pgp AS net_pd_amt_without_pgp,
  CASE WHEN claim_line_num = 1 THEN mc_tot_rpt_plan_pd_amt ELSE 0 END AS mc_tot_rpt_plan_pd_amt
FROM medicaid.claim_trans
LEFT JOIN MAT_QIP_QM_EXCL_DENOM_TCN
ON
  claim_trans.tcn = MAT_QIP_QM_EXCL_DENOM_TCN.tcn
WHERE
  latest_trans_ind = 'Y' AND
  claim_class_cd = '61' AND
  (
    claim_trans_src_cd = 'E' OR
    (claim_trans_src_cd = 'C' AND rate_cd NOT IN ('3130', '3131', '3132', '3133', '3134', '3135', '3136', '3137'))
  ) AND
  (
    pl_of_srv_bill_type_cd IN ('11', '12', '41') OR
    type_of_bill_digits_1_and_2_cd IN ('11', '12', '41')
  ) AND
  apr_drg IN (SELECT code_value FROM MAT_QIP_CODES WHERE code_type = 'DRG') AND
  dsch_dt BETWEEN (SELECT TIMESTAMPADD('year', -5, START_DATE) FROM MAT_QIP_RUN) AND (SELECT END_DATE FROM MAT_QIP_RUN)
;

DROP TABLE IF EXISTS MAT_EXCL;
CREATE LOCAL TEMPORARY TABLE MAT_EXCL
ON COMMIT PRESERVE ROWS AS
SELECT
  mbr_id,
  dsch_dt
FROM (
SELECT
  mbr_id,
  dsch_dt,
  COUNT(DISTINCT bill_npi) AS N
FROM MAT_PROV
GROUP BY 1,2
) X
WHERE
  N != 1
;

SELECT
  encounter,
  DATE_PART('year', dsch_dt) AS year,
  NVL(bill_npi,'NA') AS bill_npi,
  NVL(prov_name,'NA') AS prov_name,
  COUNT(DISTINCT mbr_id||dsch_dt) AS discharges,
  COUNT(DISTINCT CASE WHEN cesarean = 1 THEN mbr_id||DSCH_DT ELSE NULL END) AS csection_discharges,
  COUNT(DISTINCT CASE WHEN EXCL_QM_DENOM = 0 AND FIRST_BIRTH_IND = 1 THEN mbr_id||DSCH_DT ELSE NULL END) AS prev_csect_den,
  COUNT(DISTINCT CASE WHEN EXCL_QM_DENOM = 0 AND FIRST_BIRTH_IND = 1 AND cesarean = 1 THEN mbr_id||DSCH_DT ELSE NULL END) AS prev_csect_num,
  
  SUM(mc_tot_rpt_plan_pd_amt) AS mc_tot_rpt_plan_pd_amt,
  SUM(CASE WHEN cesarean = 1 THEN mc_tot_rpt_plan_pd_amt ELSE 0 END) AS csect_mc_tot_rpt_plan_pd_amt,
  SUM(CASE WHEN EXCL_QM_DENOM = 0 AND FIRST_BIRTH_IND = 1 THEN mc_tot_rpt_plan_pd_amt ELSE 0 END) AS prev_csect_den_mc_tot_rpt_plan_pd_amt,
  SUM(CASE WHEN EXCL_QM_DENOM = 0 AND FIRST_BIRTH_IND = 1 AND cesarean = 1 THEN mc_tot_rpt_plan_pd_amt ELSE 0 END) AS prev_csect_num_mc_tot_rpt_plan_pd_amt
FROM (
  SELECT
    bill_npi,
    mbr_id,
    dsch_dt,
    MIN(FIRST_BIRTH_IND) AS FIRST_BIRTH_IND,
    MAX(CASE WHEN claim_trans_src_cd = 'E' THEN 1 ELSE 0 END) AS encounter,
    MAX(EXCL_QM_DENOM) AS EXCL_QM_DENOM,
    MAX(CASE WHEN apr_drg IN ('0539', '0540') THEN 1 ELSE 0 END) AS cesarean,
    SUM(net_pd_amt_without_pgp) AS net_pd_amt_wihtout_pgp,
    SUM(mc_tot_rpt_plan_pd_amt) AS mc_tot_rpt_plan_pd_amt
  FROM MAT_PROV
  WHERE
    (mbr_id, dsch_dt) NOT IN (SELECT mbr_id, dsch_dt FROM MAT_EXCL)
  GROUP BY
    1,2,3
) X
LEFT JOIN medicaid.prov_npi Y
ON
  X.bill_npi = Y.npi
WHERE
  dsch_dt BETWEEN (SELECT START_DATE FROM MAT_QIP_RUN) AND (SELECT END_DATE FROM MAT_QIP_RUN)
GROUP BY 1,2,3,4
;

-- Professional
-- ===============================================
-- ===============================================
SELECT
  CASE WHEN claim_trans_src_cd = 'E' THEN 1 ELSE 0 END AS encounter,
  NVL(bill_npi,'NA') AS bill_npi,
  NVL(npi_prof_class_cd, 'NA') AS npi_prof_class_Cd,
  date_part('year', srv_dt) AS year,
  COUNT(*) AS claim_lines,
  COUNT(DISTINCT tcn) AS claims,
  COUNT(DISTINCT mbr_id) AS mbr,
  SUM(CASE WHEN claim_trans_src_cd = 'E' THEN mc_rpt_plan_pd_amt WHEN claim_trans_src_cd = 'C' THEN net_pd_amt_without_pgp ELSE 0 END) AS paid_amt
FROM medicaid.claim_trans CT
LEFT JOIN (
SELECT
  npi,
  MAX(npi_prof_class_cd) AS npi_prof_class_cd
FROM medicaid.prov_npi_prof
WHERE
  npi_prim_prof_ind = 'Y'
GROUP BY 1
) NPI
ON
  CT.bill_npi = NPI.npi
WHERE
  srv_dt BETWEEN (SELECT START_DATE FROM MAT_QIP_RUN) AND (SELECT END_DATE FROM MAT_QIP_RUN) AND
  claim_class_cd = '60' AND
  proc_cd_1 IN (SELECT code_value FROM MAT_QIP_CODES WHERE code_type = 'PROC') AND
  NVL(enct_pmt_type_cd,'XXX') <> 'A'
GROUP BY 1,2,3,4
;

SELECT * FROM (
SELECT
  npi,
  COUNT(*) N,
  COUNT(DISTINCT npi_prof_class_Cd) U
FROM medicaid.prov_npi_prof
WHERE
  npi_prim_prof_ind = 'Y'
GROUP BY 1
) X WHERE X.U > 1;

-- Professional Stratified by Plan
SELECT
  X.*,
  Y.plan_name_std,
  Z.N_MBR_ENR
FROM (
  SELECT
    DATE_PART('year', srv_dt) AS year,
    plan_id,
    proc_cd_1,
    COUNT(*) AS N
  FROM medicaid.claim_trans
  WHERE
    claim_trans_src_cd = 'E' AND
    srv_dt BETWEEN (SELECT START_DATE FROM MAT_QIP_RUN) AND (SELECT END_DATE FROM MAT_QIP_RUN) AND
    claim_class_cd = '60' AND
    proc_cd_1 IN (SELECT code_value FROM MAT_QIP_CODES WHERE code_type = 'PROC') AND
    NVL(enct_pmt_type_cd,'XXX') <> 'A'
  GROUP BY 1, 2, 3
) X
LEFT JOIN (
  SELECT DISTINCT
    plan_name_std,
    mc_plan_id
  FROM databook.MA_ENROLLMENT_V3
  WHERE
    LEFT(mc_ffs,1) = 'M'
) Y
ON
  X.plan_id = Y.mc_plan_id
LEFT JOIN (
  SELECT
    DATE_PART('year',calendar_date) AS year,
    mc_plan_id,
    COUNT(DISTINCT mbr_id) AS N_MBR_ENRx
  FROM databook.MA_ENROLLMENT_V3
  WHERE
    LEFT(mc_ffs,1) = 'M' AND
    DATE_PART('month', calendar_date) = 12
  GROUP BY 1,2
) Z
ON
  X.plan_id = Z.mc_plan_id AND
  X.year = Z.year
;

-- Member attribution
-- ===============================================
-- ===============================================

-- Professional
DROP TABLE IF EXISTS MAT_QIP_PROF_CLAIM;
CREATE LOCAL TEMPORARY TABLE MAT_QIP_PROF_CLAIM
ON COMMIT PRESERVE ROWS AS
SELECT
  claim_trans_id,
  claim_trans_src_cd,
  mbr_id,
  srv_dt,
  proc_cd_1,
  NVL(bill_npi,'NA') AS bill_npi,
  NVL(npi_prof_class_cd, 'NA') AS npi_prof_class_cd
FROM medicaid.claim_trans CT
LEFT JOIN (
SELECT
  npi,
  MAX(npi_prof_class_cd) AS npi_prof_class_cd
FROM medicaid.prov_npi_prof
WHERE
  npi_prim_prof_ind = 'Y'
GROUP BY 1
) NPI
ON
  CT.bill_npi = NPI.npi
WHERE
  srv_dt BETWEEN (SELECT START_DATE FROM MAT_QIP_RUN) AND (SELECT END_DATE FROM MAT_QIP_RUN) AND
  claim_class_cd = '60' AND
  proc_cd_1 IN (SELECT code_value FROM MAT_QIP_CODES WHERE code_type = 'PROC') AND
  claim_trans_src_cd = 'E' AND
  NVL(enct_pmt_type_cd,'XXX') <> 'A'
;

-- Attribution group 1 -- members with only one bill
-- ever, for any provider...
DROP TABLE IF EXISTS MAT_QIP_ATTR_LANE_1;
CREATE LOCAL TEMPORARY TABLE MAT_QIP_ATTR_LANE_1
ON COMMIT PRESERVE ROWS AS
SELECT
  A.mbr_id,
  A.srv_dt,
  A.bill_npi
FROM MAT_QIP_PROF_CLAIM A
JOIN (
  SELECT
    mbr_id,
    COUNT(*) AS N,
    COUNT(DISTINCT bill_npi) AS N_npi
  FROM MAT_QIP_PROF_CLAIM
  GROUP BY
    1
) B
ON
  A.mbr_id = B.mbr_id
WHERE
  B.N = 1
;

-- Attribution group 2 -- members with more than one
-- bill but they are on the same date for the same provider
-- likely billing artifact
DROP TABLE IF EXISTS MAT_QIP_ATTR_LANE_2;
CREATE LOCAL TEMPORARY TABLE MAT_QIP_ATTR_LANE_2
ON COMMIT PRESERVE ROWS AS
SELECT DISTINCT
  A.mbr_id,
  A.srv_dt,
  A.bill_npi
FROM MAT_QIP_PROF_CLAIM A
JOIN (
  SELECT
    mbr_id,
    COUNT(*) AS N,
    COUNT(DISTINCT bill_npi) AS N_npi
  FROM MAT_QIP_PROF_CLAIM
  GROUP BY
    1
) B
ON
  A.mbr_id = B.mbr_id
WHERE
  B.N > 1 AND
  B.N_npi = 1
ORDER BY A.mbr_id
;

-- Attribution group 3 -- members with more than one 
-- bill and more than one unique provider, but the 
-- time between the bills is > 6 months (so likely separate
-- maternity events)
SELECT
  A.*,
  C.*,
  DATEDIFF('day', A.srv_dt, C.srv_dt) AS days
FROM MAT_QIP_PROF_CLAIM A
JOIN MAT_QIP_PROF_CLAIM C
ON
  A.mbr_id = C.mbr_id AND
  A.claim_trans_id != C.claim_trans_id
JOIN (
  SELECT
    mbr_id,
    COUNT(*) AS N,
    COUNT(DISTINCT bill_npi) AS N_npi
  FROM MAT_QIP_PROF_CLAIM
  GROUP BY
    1
) B
ON
  A.mbr_id = B.mbr_id
WHERE
  B.N > 1 AND
  B.N_npi > 1
ORDER BY A.mbr_id
;

DROP TABLE IF EXISTS MAT_QIP_ATTR_LANE_3;
CREATE LOCAL TEMPORARY TABLE MAT_QIP_ATTR_LANE_3
ON COMMIT PRESERVE ROWS AS
SELECT DISTINCT
  A.mbr_id,
  A.srv_dt,
  A.bill_npi
FROM MAT_QIP_PROF_CLAIM A
JOIN MAT_QIP_PROF_CLAIM C
ON
  A.mbr_id = C.mbr_id AND
  A.claim_trans_id != C.claim_trans_id
JOIN (
  SELECT
    mbr_id,
    COUNT(*) AS N,
    COUNT(DISTINCT bill_npi) AS N_npi
  FROM MAT_QIP_PROF_CLAIM
  GROUP BY
    1
) B
ON
  A.mbr_id = B.mbr_id
WHERE
  B.N > 1 AND
  B.N_npi > 1 AND
  ABS(DATEDIFF('day', A.srv_dt, C.srv_dt)) >= 180
ORDER BY A.mbr_id
;

-- UNATTRIBUTED
-- Members with global code billings
-- that have two distinct claims/encounters
-- at separate providers that occurred w/in 180 days
-- cannot disambiguate for the purposes of attribution
SELECT
  A.mbr_id,
  A.srv_dt,
  A.bill_npi,
  C.mbr_id,
  C.srv_dt,
  C.bill_npi,
  ABS(DATEDIFF('day', A.srv_dt, C.srv_dt)) AS days_between
FROM MAT_QIP_PROF_CLAIM A
JOIN MAT_QIP_PROF_CLAIM C
ON
  A.mbr_id = C.mbr_id AND
  A.claim_trans_id != C.claim_trans_id
JOIN (
  SELECT
    mbr_id,
    COUNT(*) AS N,
    COUNT(DISTINCT bill_npi) AS N_npi
  FROM MAT_QIP_PROF_CLAIM
  GROUP BY
    1
) B
ON
  A.mbr_id = B.mbr_id
WHERE
  B.N > 1 AND
  B.N_npi > 1 AND
  ABS(DATEDIFF('day', A.srv_dt, C.srv_dt)) < 180
ORDER BY A.mbr_id
;

-- Composite attribution table
DROP TABLE IF EXISTS TMP_MAT_QIP_MBR_ATTRIBUTION;
CREATE LOCAL TEMPORARY TABLE TMP_MAT_QIP_MBR_ATTRIBUTION
ON COMMIT PRESERVE ROWS AS

SELECT *, 1 AS LANE FROM MAT_QIP_ATTR_LANE_1
UNION ALL
SELECT *, 2 AS LANE FROM MAT_QIP_ATTR_LANE_2
UNION ALL
SELECT *, 3 AS LANE FROM MAT_QIP_ATTR_LANE_3
;

DROP TABLE IF EXISTS MAT_QIP_MBR_ATTRIBUTION;
CREATE TABLE MAT_QIP_MBR_ATTRIBUTION AS
SELECT * FROM TMP_MAT_QIP_MBR_ATTRIBUTION
;

SELECT * FROM MAT_QIP_MBR_ATTRIBUTION;

-- Stragglers, those not captured in attribution
-- (126 claims of the original 75,651 in this test sample
SELECT * FROM MAT_QIP_PROF_CLAIM WHERE mbr_id NOT IN (SELECT mbr_id FROM MAT_QIP_MBR_ATTRIBUTION);
SELECT COUNT(*) FROM MAT_QIP_PROF_CLAIM;

-- Observations
SELECT COUNT(*), COUNT(DISTINCT mbr_id) FROM MAT_QIP_PROF_CLAIM WHERE mbr_id NOT IN (SELECT mbr_id FROM MAT_QIP_MBR_ATTRIBUTION) AND DATE_PART('year', srv_dt) = 2021;
SELECT COUNT(*), COUNT(DISTINCT mbr_id) FROM MAT_QIP_PROF_CLAIM WHERE DATE_PART('year', srv_dt) = 2021;
SELECT LANE, COUNT(DISTINCT mbr_id) FROM MAT_QIP_MBR_ATTRIBUTION WHERE DATE_PART('year', srv_dt) = 2021 GROUP BY 1 ORDER BY 1;

SELECT
  bill_npi,
  COUNT(*) AS N
FROM MAT_QIP_MBR_ATTRIBUTION WHERE DATE_PART('year', srv_dt) = 2021
GROUP BY 1
ORDER BY N DESC;

SELECT
  COUNT(*) AS N,
  SUM(CASE WHEN B.mbr_id IS NULL THEN 1 ELSE 0 END) AS LEFT_ONLY,
  SUM(CASE WHEN A.mbr_id IS NULL THEN 1 ELSE 0 END) AS RIGHT_ONLY,
  SUM(CASE WHEN A.mbr_id = B.mbr_id THEN 1 ELSE 0 END) AS BOTH
FROM (SELECT DISTINCT mbr_id FROM MAT_PROV WHERE claim_trans_src_cd = 'E' AND date_part('year', dsch_dt) = 2021) A
FULL OUTER JOIN (SELECT DISTINCT mbr_id FROM MAT_QIP_MBR_ATTRIBUTION WHERE DATE_PART('year', srv_dt) = 2021) B
ON
  A.mbr_id = B.mbr_id
;
