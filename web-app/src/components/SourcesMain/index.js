import { useCallback } from 'react';
import { setColor, setDateCreated, setDescription, setName } from '../../utils/setFunctions';
import { useState, useEffect } from 'react'
import './sourcesmain.css'

//This component render Description as per the version name.
const SourcesMain = ({ sourceData }) => {

    const [sourcesArray, setSourcesArray] = useState([])
    const [schemaId, setSchemaId] = useState([])
    const [schemaText, setSchemaText] = useState([])

    function schemaIdFunc(e) {
        if (schemaId.includes(e.target.id) || schemaText.includes(e.target.id)) {
            setSchemaText(schemaText.filter(schemaText => schemaText !== e.target.id))
            return setSchemaId(schemaId.filter(schemaId => schemaId !== e.target.id))
        }
        setSchemaText([...schemaText, e.target.id])
        return setSchemaId([...schemaId, e.target.id])
    }

    useEffect(() => {
        async function buildSourceDetails() {
            const objArray = []
            for (const source in sourceData.src) {
                const sourceDetails = {
                    "name": source,
                    "description": setDescription(source),
                    "dateCreated": await setDateCreated(sourceData.src[source].code.file),
                    "dateModified": sourceData.src[source].version,
                    "numberOfRecords": sourceData.src[source].stats[source],
                    "schema": { "title": "name", "cmc_unique_id": "identifier", "brief_summary": "description", "data_availability_date": "datePublished", "most_recent_update": "dateModified", "data_available": "additionalType", "creator": "funding.funder.name", "nct_number": "nctid, identifier", "condition": "healthCondition", "clinical_trial_website": "mainEntityOfPage", "publications": "citation", "data_available_for_request": "conditionsOfAccess" },
                }
                objArray.push(sourceDetails)
            }
            objArray.sort((a, b) => setName(a.name).localeCompare(setName(b.name)))
            setSourcesArray(objArray)
        }
        buildSourceDetails()
    }, [])

    const date = useCallback((data) => {
        let dateString;
        dateString = new Date(data);
        return dateString.toDateString();
    }, []);


    return (
        <div className='mb-10'>

            <div
                className="tab-content sources-title-container tab-space w-5/6 divide-y divide-light-blue-400"
                key={'key'}
            >
                <div
                    className={
                        `sources-title text-gray-900 text-2xl p-2 mt-14 w-50 my-4 ml-8 font-bold `
                    }
                >
                    Version 1.0.0 Data Sources
                </div>

            </div>

            {sourcesArray.map((sourceObj, index) => {
                return (
                    <div key={index} className={"tab-content pb-5 rounded-md border-2 border-blue-100 shadow-gray-400 shadow-sm m-2 tab-space w-5/6 divide-y divide-light-blue-400"}>
                        <div>
                            <section
                                className="flex flex-col"
                                id={`version${sourceObj.name}`}
                            >
                                <div
                                    className={`${setColor(
                                        "New Data Source"
                                    )} source-label w-36 h-auto leading-8 ml-5 text-left font-bold shadow-lg mt-10 mb-3 text-white rounded-md`}
                                >
                                    <span className="ml-4">{setName(sourceObj.name)}</span>
                                </div>
                                <div className=" ml-14 font-bold text-gray-900">
                                    {sourceObj.numberOfRecords.toLocaleString()} Records Available
                                </div>
                                <div className='ml-20 mr-20'>
                                    <div // Here we are using dangerouslySetInnerHTML because we receive the html code from API, we parse it using dangerouslySetInnerHTML
                                        className="text-left mt-4 text-gray-900"
                                        dangerouslySetInnerHTML={{
                                            __html: sourceObj.description,
                                        }}
                                    ></div>

                                    <div className='mt-2 font-bold text-gray-900'>
                                        <p> Schema showing the transformation of {setName(sourceObj.name)} Properties to the NIAID Data Ecosystem</p>
                                        {schemaText.includes(sourceObj.name) &&
                                            <button id={sourceObj.name} className='bg-green-600 hover:bg-green-800 text-white font-bold py-0 px-4 rounded mt-1 w-36' onClick={(e) => schemaIdFunc(e)}>Hide Schema</button>
                                            ||
                                            <button id={sourceObj.name} className='bg-green-600 hover:bg-green-800 text-white font-bold py-0 px-4 rounded mt-1 w-36' onClick={(e) => schemaIdFunc(e)}>Show Schema</button>
                                        }
                                        {schemaId.includes(sourceObj.name) &&
                                            <div className='mt-4 transition-test max-w-2xl relative overflow-x-auto shadow-md sm:rounded-lg'>
                                                <table className='w-full text-sm text-left text-gray-500 dark:text-gray-400'>
                                                    <thead className="text-xs text-gray-700 uppercase bg-gray-50 dark:bg-gray-700 dark:text-gray-400">
                                                        <tr>
                                                            <th scope="col" className="px-6 py-3">
                                                                {sourceObj.name} Property
                                                            </th>
                                                            <th scope="col" className="px-6 py-3">
                                                                NIAID Data Ecosystem Property
                                                            </th>
                                                        </tr>
                                                    </thead>

                                                    <tbody className='bg-white border-b dark:bg-gray-800 dark:border-gray-700'>
                                                        {Object.entries(sourceObj.schema).map((item) => {
                                                            return (
                                                                <tr key={item} className='bg-white border-b dark:bg-gray-800 dark:border-gray-700'>
                                                                    {Object.entries(item).map((field) => {
                                                                        return <td
                                                                            key={field} className='px-6 py-2 font-medium text-gray-900 dark:text-white whitespace-nowrap'>{field[1]}</td>
                                                                    })}
                                                                </tr>
                                                            );
                                                        })}
                                                    </tbody>
                                                </table>
                                            </div>
                                        }
                                    </div>
                                    <div className='mt-4 '>
                                        <div className="  font-bold text-gray-900">
                                            Latest Release {date(sourceObj.dateModified)}
                                        </div>
                                        <div className="  font-bold text-gray-900">
                                            First Released {date(sourceObj.dateCreated)}
                                        </div>
                                    </div>
                                </div>
                                <div className='text-center mt-2 mb-1'>
                                    <a href='/?' target='_blank' className='outline-none py-2 bg-transprent text-sm font-bold text-blue-500 uppercase focus:outline-none cursor-pointer'>Search {setName(sourceObj.name)} records</a>
                                </div>
                            </section>
                        </div>
                    </div>
                );
            })}

        </div >

    )
};

export default SourcesMain;
