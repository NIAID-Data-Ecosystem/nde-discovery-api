import { useCallback } from 'react';
import { setColor } from '../../utils/setFunctions';
import './main.css'

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
                                                const details = item.details;
                                                const label = item.types.label;
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
                                                        <div
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
