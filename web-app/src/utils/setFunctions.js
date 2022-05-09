export const setColor = (label) => {
    let color = "bg-red-600";
    switch (label) {
        case "Security fixes":
            color = "bg-pink-600";
            break;
        case "Bug fixes":
            color = "bg-yellow-600";
            break;
        case "Changes":
            color = "bg-green-500";
            break;
        case "New Data Source":
            color = "bg-niaid-green-500";
            break;
        case "Known issue":
            color = "bg-blue-600";
            break;
        default:
            color = "";
    }
    return color;
};

export const setDateCreated = async (sourcePath) => {
    const url = `https://api.github.com/repos/NIAID-Data-Ecosystem/nde-crawlers/commits?path=${sourcePath}`
    const response = await fetch(url)
    const jsonData = await response.json()
    const dates = []
    jsonData.forEach(jsonObj => {
        dates.push(jsonObj.commit.author.date)
    });
    return dates[dates.length - 1]
};
