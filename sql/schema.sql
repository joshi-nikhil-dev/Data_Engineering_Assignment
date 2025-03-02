
    CREATE SCHEMA australian_companies;

    CREATE TABLE australian_companies.websites (
        website_id SERIAL PRIMARY KEY,
        url VARCHAR(255) NOT NULL UNIQUE,
        company_name VARCHAR(255),
        industry VARCHAR(100),
        crawl_date DATE,
        extracted_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE australian_companies.abr_data (
        abn VARCHAR(11) PRIMARY KEY,
        company_name VARCHAR(255) NOT NULL,
        business_status VARCHAR(50),
        entity_type VARCHAR(50),
        postcode VARCHAR(4),
        updated_at TIMESTAMP
    );

    CREATE TABLE australian_companies.website_abr_mapping (
        mapping_id SERIAL PRIMARY KEY,
        website_id INT REFERENCES australian_companies.websites(website_id),
        abn VARCHAR(11) REFERENCES australian_companies.abr_data(abn),
        confidence_score FLOAT,
        UNIQUE (website_id, abn)
    );

    GRANT USAGE ON SCHEMA australian_companies TO reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA australian_companies TO reader;
    