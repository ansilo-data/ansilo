Pitching
========

## The problem

Enterprises organically develop systems and acquire technologies which:

 - store data in different (often proprietary) formats
 - speak different languages and operate over incompatible protocols
 - have different mechanisms for identity and access management
 - often have poor (or completely lack) documentation

In order to make accurate data-driven decisions they often require wholistic datasets spread amongst multiple systems.
Most organisations have a Data & Analytics vertical who is tasked with connecting, retrieving and integrating these datasets.
This is inefficient as requires low-level systems knowledge transfer across teams and is, by design, a bottleneck.
This problem only exacerbates as the organisation continues to integrate new technologies while conversely the potential value of extracting insights from the disconnected data increases.   

## The solution

Having a single queryable view for and integration point for datasets, regardless of technology, location, team or business vertical.
Where the exposed datasets (aka data products) are developed and managed alongside the systems themselves, by the team responsible for those systems. 
These are the principles of the emerging "Data Mesh" architecture.

Ansilo is designed from the ground up to be distributed and composed of nodes which are hosted, configured, scaled and deployed independently from one another. They are containerised and suited to be deployed as "side cars" alongside applications. They offer the ability to define a documented, versioned data product schema, treated as a first class interface like any other of the services's APIs. They discover each other, and provide a single catalog of data products across the organisation. They expose industry-standard protocols and authentication to easily integrate with an organisation's existing warehouse, ETL or orchestration tool, authentication mechanism and IdP. Importantly, this lets Ansilo be additive in value to the existing tech landscape, providing efficiencies within established organisations.

## The insight

Abstraction of systems behind a stable public interface is well understood in the API space however terribly understood in the data space.
No existing solution has solved for producing data abstractions (data products) when it comes to distributed systems or microservices in a way that does not violate the principle of encapsulation and ultimately lead to fragile systems/integrations.

In order to achieve this the data products must be:

 - deployed alongside their relevant services
 - following the same release process and versioning
 - designed, built and operated by the same development team

Companies that produce data warehousing/lakes ultimately depend on the centralisation of data into their platform, not only are they fundamentally tied to a bottlenecked approach, they do not solve for the encapsulation of data products from their sources.
The variety of ETL/ELT tools in market start with "E" = extract, often breaking all layers of abstraction, reaching into the guts of a service's database to retrieve what it needs, then any "abstraction" that happens later is disconnected from the source system leading to fragility.
Denodo/Starburst provide "data virtualisation" does not require the copying of data but suffers from the same lack of abstraction from its sources as ETL. 

## Describe what your company does:

 - We build software that connects data together
 - Ansilo is a tool that unifies data sources
 - We make software that unifies enterprise data
 - Ansilo is a system that unifies data across teams
 - We make software that unifies data across teams
 - Ansilo makes enterprise data uniform and accessible
 - We make enterprise data easy to manage
 - We make enterprise data easy to build and integrate
 - We lower the cost of managing enterprise data
 - We make enterprise data management cheaper
 - We make managing enterprise data cheaper
 - Ansilo democratises enterprise data management 
 - We make enterprise data management cheaper and faster
 - We make enterprise data management efficient

Longer version:

 - We make enterprise data management efficient by distributing the workload across teams.

## What is your company going to make? Please describe your product and what it does or will do.


## Topline description of Ansilo:

What if moving your data was as simple to construct, audit and maintain as a single SQL query?
    
    "INSERT INTO teradata.t1.contacts_srchst SELECT * FROM openshift.contact_service.contacts"
    "INSERT INTO aws.s3.combined_contact_history SELECT * FROM teradata.t3.combined_contact_history"

Ansilo is the only distributed platform that unifies the process for developing, discovering, querying and moving data, regardless of on-prem/cloud, database vendor or software stack.
Its unique distributed design allows it to integrate seamlessly across microservices and monolithic data platforms alike.
Its data abstractions features allows for truly segregated ownership of data and full autonomy across teams but central policies ensure consistent standards and access across the organisation.

=====

Notes for ANZ:

 - Agnostic system (not locked in to single vendor)

 Actions

[x] get back to Mark with topline product description.
[ ] tooltips for data flow
[x] legend for ERD
[ ] equity sharing alternatives
[ ] refine the pitch decks
[ ] rehearsing!
