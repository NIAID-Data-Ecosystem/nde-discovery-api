import { useQuery } from "@apollo/client";
import { GET_VERSIONS } from "../../gql/query";
import Header from "../Header";
import Sidebar from "../Sidebar";
import Main from "../Main";

const UI = () => {
    const { data = {}, loading } = useQuery(GET_VERSIONS);
    return (
        <>
            <Header />
            <div className="min-h-screen flex flex-row">
                <div className="flex flex-col bg-gray-100   text-white w-6/12 mobile:hidden">
                    <ul className="flex flex-col py-4 sticky top-0  divide-y divide-y divide-gray-400">
                        <Sidebar versions={data.versions} loading={loading} />
                    </ul>
                </div>
                <div className="main-container">
                    <Main versions={data.versions} loading={loading} />
                </div>
            </div>
        </>
    );
};

export default UI;
