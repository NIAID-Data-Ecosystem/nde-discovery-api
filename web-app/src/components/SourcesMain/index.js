import { useCallback } from 'react';
import { setColor, setDescription, setName } from '../../utils/setFunctions';
import './sourcesmain.css'

//This component render Description as per the version name.
const SourcesMain = ({ sourceData }) => {

    const sourcesArray = []
    for (const source in sourceData.src) {
        const sourceDetails = {
            "name": source,
            "description": setDescription(source),
            "dateCreated": "",
            "dateModified": sourceData.src[source].version,
            "numberOfRecords": sourceData.src[source].stats[source],
            "schema": "",
        }
        sourcesArray.push(sourceDetails)
    }
    console.log(sourcesArray)


    const date = useCallback((data) => {
        let dateString;
        dateString = new Date(data);
        return dateString.toDateString();
    }, []);


    return (
        <div>

            <div
                className="tab-content sources-title-container tab-space w-5/6 divide-y divide-light-blue-400"
                key={'key'}
            >
                <div
                    className={
                        `sources-title text-gray-900 text-2xl p-2 mt-14 w-50 my-4 ml-8 font-bold `
                    }
                >
                    Sources
                </div>

            </div>

            {sourcesArray.map((sourceObj, index) => {
                return (
                    <div className={"tab-content tab-space w-5/6 divide-y divide-light-blue-400"}>
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
                                <div // Here we are using dangerouslySetInnerHTML because we receive the html code from API, we parse it using dangerouslySetInnerHTML
                                    className="text-left ml-20 mt-4 text-gray-900"
                                    dangerouslySetInnerHTML={{
                                        __html: sourceObj.description,
                                    }}
                                ></div>
                                <div className='mt-4 ml-5'>
                                    <div className=" ml-14 font-bold text-gray-900">
                                        Date Modified {date(sourceObj.dateModified)}
                                    </div>
                                    <div className=" ml-14 font-bold text-gray-900">
                                        Date Created {date(sourceObj.dateModified)}
                                    </div>
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
