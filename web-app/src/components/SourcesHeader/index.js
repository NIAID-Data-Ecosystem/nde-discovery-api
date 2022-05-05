import './sourcesheader.css'
import logo from './logo.svg'
const SourcesHeader = () => {
    return (
        <nav className="release-header flex items-center justify-between flex-wrap bg-blue-600  p-6">
            <div className="flex items-center flex-no-shrink text-white mr-6 w-full justify-start">
                <img className='logo' src={logo} />
                <span className="header-title font-semibold text-xl tracking-tight">
                    NIAID Data Ecosystem Discovery API Sources
                </span>
            </div>
        </nav>
    );
};

export default SourcesHeader;
