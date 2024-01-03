FROM mcr.microsoft.com/dotnet/sdk:8.0 as build
WORKDIR /build
RUN echo "revision 37a7e96bdc9cb7d0c6e9a1e280e37297a975a118b"
RUN git clone --depth=1 https://github.com/Coflnet/HypixelSkyblock.git dev
WORKDIR /build/sky
COPY SkyFlipper.csproj SkyFlipper.csproj
RUN dotnet restore
COPY . .
RUN dotnet test
RUN dotnet publish -c release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app

COPY --from=build /app .
RUN mkdir -p ah/files
ENV ASPNETCORE_URLS=http://+:8000

RUN useradd --uid $(shuf -i 2000-65000 -n 1) app
USER app

ENTRYPOINT ["dotnet", "SkyFlipper.dll", "--hostBuilder:reloadConfigOnChange=false"]

VOLUME /data
