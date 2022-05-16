import { useCallback, useEffect, useState } from 'react';
import { setColor, setDateCreated } from '../../utils/setFunctions';
import './main.css'

//This component render Description as per the version name.
const Main = ({ sourceData }) => {

    const [sourcesArray, setSourcesArray] = useState([])
    const [schemaId, setSchemaId] = useState([])
    const [schemaText, setSchemaText] = useState([])

    function schemaIdFunc(e) {
        if (schemaId.includes(e.target.id) || schemaText.includes(e.target.id)) {
            setSchemaText(schemaText.filter(schemaText => schemaText !== e.target.id));
            return setSchemaId(schemaId.filter(schemaId => schemaId !== e.target.id));
        };
        setSchemaText([...schemaText, e.target.id]);
        return setSchemaId([...schemaId, e.target.id]);
    }

    useEffect(() => {
        async function buildSourceDetails() {
            const objArray = [];
            for (const source in sourceData.src) {
                const sourceDetails = {
                    "name": sourceData.src[source].sourceInfo.name,
                    "description": sourceData.src[source].sourceInfo.description,
                    "dateCreated": await setDateCreated(sourceData.src[source].code.file),
                    "dateModified": sourceData.src[source].version,
                    "numberOfRecords": sourceData.src[source].stats[source],
                    "schema": sourceData.src[source].sourceInfo.schema,
                };
                objArray.push(sourceDetails);
            };
            objArray.sort((a, b) => a.name.localeCompare(b.name));
            setSourcesArray(objArray);
        };
        buildSourceDetails();
    }, []);

    const date = useCallback((data) => {
        let dateString;
        dateString = new Date(data);
        return dateString.toDateString();
    }, []);


    return (
        <div className='mb-10'>

            <div
                className="tab-content tab-space md:w-5/6 divide-y divide-light-blue-400 border-b-2"
                key={'key'}
            >
                <div
                    className={
                        `text-gray-900 text-2xl p-2 mt-14 w-50 my-4 md:ml-8 font-bold `
                    }
                >
                    Version 1.0.0 Data Sources
                </div>

            </div>

            {sourcesArray.map((sourceObj, index) => {
                return (
                    <div key={index} className={"tab-content pb-5 rounded-md border-2 border-niaid-blue/20 shadow-gray-400 shadow-sm m-2 tab-space md:w-5/6 divide-y divide-light-blue-400"}>
                        <div>
                            <section
                                className="flex flex-col"
                                id={`version${sourceObj.name}`}
                            >
                                <div
                                    className={`bg-niaid-green-500 h-auto leading-8 ml-2 md:ml-5 text-left font-bold shadow-lg mt-10 mb-3 text-white w-96 mr-2 rounded-md`}
                                >
                                    <span className="ml-4">New Data Source</span>
                                </div>
                                <div className=" md:ml-14 font-bold text-gray-900">
                                    {sourceObj.name} ({sourceObj.numberOfRecords.toLocaleString()} Records Available)
                                </div>
                                <div className='md:ml-20 md:mr-20 ml-2 mr-2'>
                                    <div
                                        className="md:text-left mt-4 text-justify text-gray-900"
                                        dangerouslySetInnerHTML={{
                                            __html: sourceObj.description,
                                        }}
                                    ></div>
                                </div>
                            </section>
                        </div>
                    </div>
                );
            })}

        </div >

    )
};

export default Main;
