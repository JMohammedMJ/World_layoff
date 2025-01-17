SELECT *
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM  layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffS;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffS_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffS_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layoffs_staging2` (
	`company` text,
    `location` text,
    `industry` text,
    `total_laid_off` int DEFAULT NULL,
    `percentage_laid_off` text,
    `date` text,
    `country` text,
    `funds_raised_millions` int DEFAULT NULL,
    `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM  layoffs_staging2;

INSERT INTO layoffs_staging2 (
    company, 
    location, 
    industry, 
    total_laid_off, 
    percentage_laid_off, 
    date, 
    country, 
    funds_raised_millions, 
    row_num
)
SELECT 
    company, 
    location, 
    industry, 
    total_laid_off, 
    percentage_laid_off, 
    date, 
    country, 
    funds_raised_millions,
    ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, 
        percentage_laid_off, `date`, stage, country, funds_raised_millions
        ORDER BY company  -- Add ordering condition, required for ROW_NUMBER()
    ) AS row_num
FROM layoffs_staging;
-- Cleaning Data
SELECT *
FROM  layoffs_staging2
WHERE row_num > 1;

DELETE
FROM  layoffs_staging2
WHERE row_num > 1 ;

SELECT *
FROM  layoffs_staging2;

-- Standarzing Data

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE  layoffs_staging2
SET  company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2; 

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET Country = TRIM(TRAILING '.' FROM country)
WHERE Country LIKE 'United States%';

SELECT `date`,
str_to_date(`DATE`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`DATE`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE  layoffs_staging2
MODIFY COLUMN  `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE COMPANY = 'Airbnb';

SELECT *
FROM layoffs_staging2
WHERE Company  LIKE 'Bally%';

SELECT T1.industry, T2.industry
FROM layoffs_staging2 T1
JOIN layoffs_staging2 t2
	ON T1.company = T2.company
    AND T1.location = T2.location
WHERE (T1.industry IS NULL OR T1.industry = '')
AND T2.industry IS NOT NULL;

UPDATE layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company = T2.company
SET T1.industry = T2.industry
WHERE T1.industry IS NULL
AND T2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;



















