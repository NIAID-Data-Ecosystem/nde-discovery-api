import { useCallback } from 'react';
import { setColor } from '../../utils/setFunctions';
import './main.css'

//This component render Description as per the version name.
const Main = ({ versions, loading }) => {

    const date = useCallback((data) => {
        let dateString;
        dateString = new Date(data);
        return dateString.toDateString();
    }, []);


    return (
        <>
            {versions &&
                !loading &&
                versions
                    .map((data, index) => {
                        return (
                            <div
                                className="tab-content tab-space w-5/6 divide-y divide-light-blue-400"
                                key={index}
                            >
                                <div
                                    className={
                                        `version-title version${data.version} text-gray-900 text-2xl p-2 mt-14 w-50 my-4 ml-8 font-bold`
                                    }
                                >
                                    Version {data.version}
                                    <div className="text-sm font-medium text-gray-500">
                                        {date(data.date)}
                                    </div>
                                </div>
                                <div className={"block"}>
                                    <div>
                                        {data.description
                                            .map((item) => {
                                                // The Description in our API is a 'field set' and it two fields, 1. Details and  2. Label
                                                const details = item.details; // Here we are accessing the details, details contains all the text.
                                                const label = item.types.label; // Here we are accessing picker option's label. There are total 4 labels. 1. Security Fixes 2. Bug fixes, 3. Changes and 4. Know issue
                                                return (
                                                    <section
                                                        className="flex flex-col  mb-10"
                                                        id={`version${data.version}`}
                                                    >
                                                        <div
                                                            className={`${setColor(
                                                                label
                                                            )} version-label w-36 h-auto leading-8 ml-10 text-left font-bold shadow-lg mt-10 mb-3 text-white rounded-md`}
                                                        >
                                                            <span className="ml-4">{label}</span>
                                                        </div>
                                                        <div // Here we are using dangerouslySetInnerHTML because we receive the html code from API, we parse it using dangerouslySetInnerHTML
                                                            className="text-left ml-20 mt-4 text-gray-900"
                                                            dangerouslySetInnerHTML={{
                                                                __html: details,
                                                            }}
                                                        ></div>
                                                    </section>
                                                );
                                            })
                                            .reverse()}
                                    </div>
                                </div>
                            </div>
                        );
                    })
                    .reverse()}
        </>
    );
};

export default Main;
