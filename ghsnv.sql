CREATE TABLE GHSNV
(
	RecordId bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
	SampleId varchar(255),
	RunId varchar(255),
	Gene varchar(255),
	Mutation_AA varchar(255),
	Percentage double,
	Chrom int,
	Position bigint
);