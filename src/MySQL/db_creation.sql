-- Verificar si la base de datos no existe y crearla
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'cyber_jobs')
BEGIN
    CREATE DATABASE cyber_jobs;
END
GO

-- Cambiar el contexto a la nueva base de datos
USE cyber_jobs;
GO

-- Crear la tabla para los salarios si no existe
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = N'cyber_salaries')
BEGIN
    CREATE TABLE cyber_salaries (
        row_id NVARCHAR(255) PRIMARY KEY,
        rank_title NVARCHAR(255),
        role_normalized NVARCHAR(255),
        company_std NVARCHAR(255),
        location NVARCHAR(255),
        location_std NVARCHAR(255),
        pay_in NVARCHAR(255),
        salary_clean DECIMAL(15, 2),
        salary_band NVARCHAR(50),
        experience_level NVARCHAR(100),
        years_experience_num INT,
        certifications_required NVARCHAR(MAX)
    );
END
GO